package server

import (
	"fmt"
	"io"
	"net"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/store"
	"github.com/orkunkaraduman/zelus/pkg/utils"
	"github.com/orkunkaraduman/zelus/pkg/wrh"
)

type connState struct {
	srv  *Server
	conn net.Conn
	*protocol.Protocol
	bfs        []*buffer.Buffer
	cb         chan interface{}
	cbLen      int
	respNodes  []wrh.Node
	respNodes2 []wrh.Node
	standalone bool

	rCmd protocol.Cmd
}

const statsStr = `Key Count: %s
Keyspace size: %s
Dataspace size: %s
Requested Operation Count: %s
Successful Operation Count: %s
Slot Count: %s
`

const (
	connectTimeout    = 1 * time.Second
	pingTimeout       = 1 * time.Second
	connectRetryCount = 3
	queueGroupMaxLen  = 2560
	queueGroupMaxSize = 10 * 1024 * 1024
)

func newConnState(srv *Server, conn net.Conn) (cs *connState) {
	cs = &connState{
		srv:        srv,
		conn:       conn,
		Protocol:   protocol.New(conn, conn),
		bfs:        make([]*buffer.Buffer, 0, maxKeyCount*maxRespNodes),
		cb:         make(chan interface{}, maxKeyCount*maxRespNodes),
		respNodes:  make([]wrh.Node, 0, maxRespNodes),
		respNodes2: make([]wrh.Node, 0, maxRespNodes),
	}
	return
}

func (cs *connState) setRespNodes() {
	var u uint
	u = 1 + cs.srv.nodeBackups
	cs.respNodes = cs.respNodes[:u]
	u = 1 + cs.srv.nodeBackups2
	cs.respNodes2 = cs.respNodes2[:u]
}

func (cs *connState) cmdPing() (count int) {
	var err error
	err = cs.SendCmd(protocol.Cmd{Name: "PONG"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdStandalone() (count int) {
	var err error
	cs.standalone = true
	err = cs.SendCmd(protocol.Cmd{Name: "STANDALONE"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdStats() (count int) {
	var err error
	stats := cs.srv.st.Stats()
	args := []string{
		strconv.FormatInt(stats.KeyCount, 10),
		strconv.FormatInt(stats.KeyspaceSize, 10),
		strconv.FormatInt(stats.DataspaceSize, 10),
		strconv.FormatInt(stats.ReqOperCount, 10),
		strconv.FormatInt(stats.SucOperCount, 10),
		strconv.FormatInt(stats.SlotCount, 10),
	}
	str := fmt.Sprintf(statsStr,
		args[0],
		args[1],
		args[2],
		args[3],
		args[4],
		args[5],
	)
	err = cs.SendCmd(protocol.Cmd{Name: "STATS", Args: args})
	if err != nil {
		panic(err)
	}
	err = cs.SendData([]byte(str), -1)
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdDebugStack() (count int) {
	var err error
	err = cs.SendCmd(protocol.Cmd{Name: "OK"})
	if err != nil {
		panic(err)
	}
	var buf [102400]byte
	err = cs.SendData(buf[:runtime.Stack(buf[:], true)], 0)
	if err != nil {
		panic(err)
	}
	return
}

const (
	serrCmdClusterArgCount     = "invalid argument count"
	serrCmdClusterInvalidState = "invalid state"
	serrCmdClusterCannotSet    = "can not set to another node"
)

func (cs *connState) cmdClusterInit() (count int) {
	var err error
	if len(cs.rCmd.Args) < 2 {
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterArgCount}})
		if err != nil {
			panic(err)
		}
		return
	}
	cs.srv.nodesMu.Lock()
	cs.srv.clusterState = clusterStateNormal
	var u uint64
	u, err = strconv.ParseUint(cs.rCmd.Args[0], 10, 32)
	if err != nil {
		u = 0
	}
	cs.srv.nodeID = uint32(u)
	u, err = strconv.ParseUint(cs.rCmd.Args[1], 10, 32)
	if err != nil {
		u = 0
	}
	cs.srv.nodeBackups = uint(u)
	cs.srv.nodeBackups2 = uint(u)
	nodeCount := (len(cs.rCmd.Args) - 2) / 2
	args := make([]string, 0, nodeCount)
	cs.srv.nodes = make([]wrh.Node, 0, nodeCount)
	cs.srv.nodes2 = make([]wrh.Node, 0, nodeCount*2)
	for _, q := range cs.srv.nodeQueueGroups {
		q.Close()
	}
	cs.srv.nodeQueueGroups = make(map[uint32]*queueGroup, nodeCount*2)
	for i := 0; i < nodeCount; i++ {
		idx := 2 + i*2
		var u uint64
		u, err = strconv.ParseUint(cs.rCmd.Args[idx+0], 10, 32)
		if err != nil {
			u = 0
		}
		id := uint32(u)
		addr := cs.rCmd.Args[idx+1]
		if wrh.FindSeed(cs.srv.nodes2, id) >= 0 {
			continue
		}
		nd := wrh.Node{
			Seed:   id,
			Weight: 1.0,
		}
		cs.srv.nodes = append(cs.srv.nodes, nd)
		cs.srv.nodes2 = append(cs.srv.nodes2, nd)
		cs.srv.nodeQueueGroups[id] = newQueueGroup(addr, connectTimeout, pingTimeout, connectRetryCount, queueGroupMaxLen, queueGroupMaxSize)
		args = append(args, strconv.FormatUint(uint64(id), 10), addr)
	}
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: args})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdClusterNodeadd() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
	if cs.srv.clusterState != clusterStateNormal {
		cs.srv.nodesMu.Unlock()
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterInvalidState}})
		if err != nil {
			panic(err)
		}
		return
	}
	nodeCount := len(cs.rCmd.Args) / 2
	args := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		idx := i * 2
		var u uint64
		u, err = strconv.ParseUint(cs.rCmd.Args[idx+0], 10, 32)
		if err != nil {
			u = 0
		}
		id := uint32(u)
		addr := cs.rCmd.Args[idx+1]
		if wrh.FindSeed(cs.srv.nodes2, id) >= 0 {
			continue
		}
		nd := wrh.Node{
			Seed:   id,
			Weight: 1.0,
		}
		cs.srv.nodes2 = append(cs.srv.nodes2, nd)
		if _, ok := cs.srv.nodeQueueGroups[id]; !ok {
			cs.srv.nodeQueueGroups[id] = newQueueGroup(addr, connectTimeout, pingTimeout, connectRetryCount, queueGroupMaxLen, queueGroupMaxSize)
		} else {
			cs.srv.nodeQueueGroups[id].remove = false
		}
		args = append(args, strconv.FormatUint(uint64(id), 10), addr)
	}
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: args})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdClusterNoderm() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
	if cs.srv.clusterState != clusterStateNormal {
		cs.srv.nodesMu.Unlock()
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterInvalidState}})
		if err != nil {
			panic(err)
		}
		return
	}
	nodeCount := len(cs.rCmd.Args)
	args := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		var u uint64
		u, err = strconv.ParseUint(cs.rCmd.Args[i], 10, 32)
		if err != nil {
			u = 0
		}
		id := uint32(u)
		ndIdx2 := wrh.FindSeed(cs.srv.nodes2, id)
		if ndIdx2 < 0 {
			continue
		}
		cs.srv.nodes2 = cs.srv.nodes2[:ndIdx2+copy(cs.srv.nodes2[ndIdx2:], cs.srv.nodes2[ndIdx2+1:])]
		cs.srv.nodeQueueGroups[id].remove = true
		args = append(args, strconv.FormatUint(uint64(id), 10))
	}
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: args})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdClusterReshard() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
	if cs.srv.clusterState != clusterStateReshardWait {
		cs.srv.nodesMu.Unlock()
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterInvalidState}})
		if err != nil {
			panic(err)
		}
		return
	}
	cs.srv.clusterState = clusterStateReshard
	cs.srv.nodesMu.Unlock()
	bf := cs.srv.bfPool.GetOrNew()
	cs.srv.nodesMu.RLock()
	cs.setRespNodes()
	var serr []string
	var val []byte
	cs.srv.st.Scan(func(key string, size int, index int, data []byte, expiry int) (cont bool) {
		if index == 0 {
			val = bf.Want(size)
		}
		if index+copy(val[index:], data) >= size {
			pkey := utils.StringToByteSlice(key)
			wrh.ResponsibleNodes(cs.srv.nodes, pkey, cs.respNodes)
			wrh.ResponsibleNodes(cs.srv.nodes2, pkey, cs.respNodes2)
			masterID := wrh.MaxScore(cs.respNodes)
			if cs.srv.nodeID == masterID {
				kv := keyVal{
					Key:      key,
					Val:      val,
					Expires:  toExpires(expiry),
					CallBack: cs.cb,
				}
				for i, j := 0, len(cs.respNodes2); i < j; i++ {
					id := cs.respNodes2[i].Seed
					if cs.srv.nodeID == id {
						continue
					}
					q := cs.srv.nodeQueueGroups[id]
					q.nodeSetQueue.Add(kv)
					e, _ := (<-cs.cb).(error)
					if e != nil {
						serr = []string{serrCmdClusterCannotSet, strconv.FormatUint(uint64(id), 10), q.addr}
						return false
					}
				}
			}
		}
		return true
	})
	cs.srv.nodesMu.RUnlock()
	cs.srv.bfPool.Put(bf)
	if serr != nil {
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: serr})
		if err != nil {
			panic(err)
		}
		return
	}
	err = cs.SendCmd(protocol.Cmd{Name: "OK"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdClusterClean() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
	if cs.srv.clusterState != clusterStateCleanWait {
		cs.srv.nodesMu.Unlock()
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterInvalidState}})
		if err != nil {
			panic(err)
		}
		return
	}
	cs.srv.clusterState = clusterStateClean
	cs.srv.nodeBackups = cs.srv.nodeBackups2
	cs.srv.nodes = cs.srv.nodes2
	cs.srv.nodes2 = make([]wrh.Node, 0, len(cs.srv.nodes)*2)
	for i := range cs.srv.nodes {
		cs.srv.nodes2 = append(cs.srv.nodes2, cs.srv.nodes[i])
	}
	for i, q := range cs.srv.nodeQueueGroups {
		if q.remove {
			q.Close()
			delete(cs.srv.nodeQueueGroups, i)
		}
	}
	cs.srv.nodesMu.Unlock()
	cs.srv.nodesMu.RLock()
	cs.setRespNodes()
	cs.srv.st.Scan(func(key string, size int, index int, data []byte, expiry int) (cont bool) {
		if index+len(data) >= size {
			pkey := utils.StringToByteSlice(key)
			wrh.ResponsibleNodes(cs.srv.nodes2, pkey, cs.respNodes2)
			if wrh.FindSeed(cs.respNodes2, cs.srv.nodeID) < 0 {
				go cs.srv.st.Del(key)
			}
		}
		return true
	})
	cs.srv.nodesMu.RUnlock()
	cs.srv.nodesMu.Lock()
	cs.srv.clusterState = clusterStateNormal
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdClusterWait() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
	if cs.srv.clusterState != clusterStateNormal && cs.srv.clusterState != clusterStateReshard {
		cs.srv.nodesMu.Unlock()
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterInvalidState}})
		if err != nil {
			panic(err)
		}
		return
	}
	cs.srv.clusterState++
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdGet() (count int) {
	var err error
	for _, key := range cs.rCmd.Args {
		if key == "" {
			cs.cb <- keyVal{
				Key:      key,
				Val:      nil,
				Expires:  -1,
				CallBack: cs.cb,
			}
			cs.cbLen++
			continue
		}
		var useStore bool
		for {
			cs.srv.nodesMu.RLock()
			if cs.srv.clusterState == clusterStateReshardWait || cs.srv.clusterState == clusterStateCleanWait {
				cs.srv.nodesMu.RUnlock()
				time.Sleep(5 * time.Millisecond)
			} else {
				break
			}
		}
		if cs.srv.clusterState == clusterStateNonclustered || cs.standalone {
			useStore = true
		} else {
			cs.setRespNodes()
			pkey := utils.StringToByteSlice(key)
			wrh.ResponsibleNodes(cs.srv.nodes, pkey, cs.respNodes)
			masterID := wrh.MaxScore(cs.respNodes)
			if cs.srv.nodeID == masterID {
				useStore = true
			} else {
				bf := cs.srv.bfPool.GetOrNew()
				cs.bfs = append(cs.bfs, bf)
				q := cs.srv.nodeQueueGroups[masterID]
				q.nodeGetQueue.Add(keyVal{
					Key:      key,
					Val:      nil,
					Expires:  -1,
					CallBack: cs.cb,
					UserData: bf,
				})
				cs.cbLen++
			}
		}
		cs.srv.nodesMu.RUnlock()
		if useStore {
			var val []byte
			var expires int
			bf := cs.srv.bfPool.GetOrNew()
			cs.bfs = append(cs.bfs, bf)
			if cs.srv.st.Get(key, func(size int, index int, data []byte, expiry int) (cont bool) {
				if index == 0 {
					val = bf.Want(size)
					expires = toExpires(expiry)
				}
				copy(val[index:], data)
				return
			}) {
				cs.cb <- keyVal{
					Key:      key,
					Val:      val,
					Expires:  expires,
					CallBack: cs.cb,
				}
				cs.cbLen++
			} else {
				cs.cb <- keyVal{
					Key:      key,
					Val:      nil,
					Expires:  -1,
					CallBack: cs.cb,
				}
				cs.cbLen++
			}
		}
	}
	results := make(map[string]keyVal, len(cs.rCmd.Args))
	for cs.cbLen > 0 {
		if kv, ok := (<-cs.cb).(keyVal); ok {
			if _, ok := kv.UserData.(error); !ok {
				if _, ok := results[kv.Key]; !ok {
					results[kv.Key] = kv
				}
			}
		}
		cs.cbLen--
	}
	keys := make([]string, 0, len(cs.rCmd.Args))
	for _, key := range cs.rCmd.Args {
		if _, ok := results[key]; ok {
			keys = append(keys, key)
		}
	}
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: keys})
	if err == nil {
		for _, key := range keys {
			kv := results[key]
			err = cs.SendData(kv.Val, kv.Expires)
			if err != nil {
				break
			}
		}
	}
	for _, bf := range cs.bfs {
		cs.srv.bfPool.Put(bf)
	}
	cs.bfs = cs.bfs[:0]
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdSet() (count int) {
	var err error
	count = len(cs.rCmd.Args)
	if count > 0 {
		return
	}
	err = cs.SendCmd(protocol.Cmd{Name: "OK"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmddataSet(count int, index int, data []byte, expires int) {
	var err error
	key := cs.rCmd.Args[index]
	if key != "" {
		var useStore bool
		var f store.GetFunc
		for {
			cs.srv.nodesMu.RLock()
			if cs.srv.clusterState == clusterStateReshardWait || cs.srv.clusterState == clusterStateCleanWait {
				cs.srv.nodesMu.RUnlock()
				time.Sleep(5 * time.Millisecond)
			} else {
				break
			}
		}
		if cs.srv.clusterState == clusterStateNonclustered || cs.standalone {
			var val []byte
			bf := cs.srv.bfPool.GetOrNew()
			cs.bfs = append(cs.bfs, bf)
			useStore = true
			f = func(size int, index int, data []byte, expiry int) (cont bool) {
				if index == 0 {
					val = bf.Want(size)
				}
				if data == nil || index+copy(val[index:], data) >= size {
					kv := keyVal{
						Key:      key,
						Val:      val,
						Expires:  expires,
						CallBack: cs.cb,
					}
					cs.cb <- kv
					cs.cbLen++
				}
				return true
			}
		} else {
			cs.setRespNodes()
			pkey := utils.StringToByteSlice(key)
			wrh.ResponsibleNodes(cs.srv.nodes, pkey, cs.respNodes)
			masterID := wrh.MaxScore(cs.respNodes)
			if cs.srv.nodeID == masterID {
				var mergedRespNodes []wrh.Node
				if cs.srv.clusterState != clusterStateReshard {
					mergedRespNodes = cs.respNodes
				} else {
					wrh.ResponsibleNodes(cs.srv.nodes2, pkey, cs.respNodes2)
					mergedRespNodes = wrh.MergeNodes(cs.respNodes, cs.respNodes2, make([]wrh.Node, 0, len(cs.respNodes)+len(cs.respNodes2)))
				}
				var val []byte
				bf := cs.srv.bfPool.GetOrNew()
				cs.bfs = append(cs.bfs, bf)
				useStore = true
				f = func(size int, index int, data []byte, expiry int) (cont bool) {
					if index == 0 {
						val = bf.Want(size)
					}
					if data == nil || index+copy(val[index:], data) >= size {
						kv := keyVal{
							Key:      key,
							Val:      val,
							Expires:  expires,
							CallBack: cs.cb,
						}
						cs.cb <- kv
						cs.cbLen++
						for i, j := 0, len(mergedRespNodes); i < j; i++ {
							id := mergedRespNodes[i].Seed
							if cs.srv.nodeID == id {
								continue
							}
							q := cs.srv.nodeQueueGroups[id]
							q.nodeSetQueue.Add(kv)
							cs.cbLen++
						}
					}
					return true
				}
			} else {
				var val []byte
				if data != nil {
					bf := cs.srv.bfPool.GetOrNew()
					cs.bfs = append(cs.bfs, bf)
					val = bf.Want(len(data))
					copy(val, data)
				}
				kv := keyVal{
					Key:      key,
					Val:      val,
					Expires:  expires,
					CallBack: cs.cb,
				}
				q := cs.srv.nodeQueueGroups[masterID]
				switch cs.rCmd.Name {
				case "SET":
					q.masterSetQueue.Add(kv)
					cs.cbLen++
				case "PUT":
					q.masterPutQueue.Add(kv)
					cs.cbLen++
				case "APPEND":
					q.masterAppendQueue.Add(kv)
					cs.cbLen++
				case "DEL":
					q.masterSetQueue.Add(kv)
					cs.cbLen++
				}
			}
		}
		if useStore {
			var result bool
			expiry := toExpiry(expires)
			switch cs.rCmd.Name {
			case "SET":
				result = cs.srv.st.Set(key, data, expiry, f)
			case "PUT":
				result = cs.srv.st.Put(key, data, expiry, f)
			case "APPEND":
				result = cs.srv.st.Append(key, data, expiry, f)
			case "DEL":
				result = cs.srv.st.Set(key, data, expiry, f)
			}
			runtime.KeepAlive(result)
		}
		cs.srv.nodesMu.RUnlock()
	}
	if index+1 >= count {
		results := make(map[string]int, len(cs.rCmd.Args))
		for cs.cbLen > 0 {
			if kv, ok := (<-cs.cb).(keyVal); ok {
				if _, ok := kv.UserData.(error); !ok {
					if _, ok := results[kv.Key]; !ok {
						results[kv.Key] = 0
					}
				} else {
					if _, ok := results[kv.Key]; ok {
						results[kv.Key]++
					}
				}
			}
			cs.cbLen--
		}
		for _, bf := range cs.bfs {
			cs.srv.bfPool.Put(bf)
		}
		cs.bfs = cs.bfs[:0]
		cmdName := "OK"
		keys := make([]string, 0, len(cs.rCmd.Args))
		for _, key := range cs.rCmd.Args {
			if errCount, ok := results[key]; ok {
				keys = append(keys, key)
				if errCount != 0 {
					cmdName = "WARN"
				}
			}
		}
		err = cs.SendCmd(protocol.Cmd{Name: cmdName, Args: keys})
		if err != nil {
			panic(err)
		}
	}
	return
}

func (cs *connState) cmdDel() (count int) {
	cnt := len(cs.rCmd.Args)
	for index := range cs.rCmd.Args {
		cs.cmddataSet(cnt, index, nil, -1)
	}
	return
}

func (cs *connState) OnReadCmd(cmd protocol.Cmd) (count int) {
	cs.rCmd = cmd
	if cs.rCmd.Name == "QUIT" || cs.rCmd.Name == "FATAL" {
		count = -1
		return
	}
	if cs.rCmd.Name == "PING" {
		return cs.cmdPing()
	}
	if cs.rCmd.Name == "STANDALONE" {
		return cs.cmdStandalone()
	}
	if cs.rCmd.Name == "STATS" {
		return cs.cmdStats()
	}
	if cs.rCmd.Name == "DEBUG" {
		subCmd := strings.ToUpper(cs.rCmd.Args[0])
		cs.rCmd.Args = cs.rCmd.Args[1:]
		switch subCmd {
		case "STACK":
			return cs.cmdDebugStack()
		}
	}
	if cs.rCmd.Name == "CLUSTER" && len(cs.rCmd.Args) >= 1 {
		subCmd := strings.ToUpper(cs.rCmd.Args[0])
		cs.rCmd.Args = cs.rCmd.Args[1:]
		switch subCmd {
		case "INIT":
			return cs.cmdClusterInit()
		case "NODEADD":
			return cs.cmdClusterNodeadd()
		case "NODERM":
			return cs.cmdClusterNoderm()
		case "RESHARD":
			return cs.cmdClusterReshard()
		case "CLEAN":
			return cs.cmdClusterClean()
		case "WAIT":
			return cs.cmdClusterWait()
		}
	}
	if cs.rCmd.Name == "GET" {
		return cs.cmdGet()
	}
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
		return cs.cmdSet()
	}
	if cs.rCmd.Name == "DEL" {
		return cs.cmdDel()
	}
	panic(&protocol.Error{Err: ErrProtocolUnexpectedCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(count int, index int, data []byte, expires int) {
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
		cs.cmddataSet(count, index, data, expires)
		return
	}
}

func (cs *connState) OnQuit(e error) {
	defer func() {
		cs.Flush()
	}()
	if _, ok := e.(net.Error); !ok && e != nil && e != io.EOF {
		cmd := protocol.Cmd{Name: "FATAL"}
		if e, ok := e.(*protocol.Error); ok {
			cmd.Args = append(cmd.Args, e.Err.Error())
			if e.Err == ErrProtocolUnexpectedCommand {
				cmd.Args = append(cmd.Args, e.Cmd.Name)
			}
		} else {
			debug.PrintStack()
		}
		cs.SendCmd(cmd)
		return
	}
	cs.SendCmd(protocol.Cmd{Name: "QUIT"})
}
