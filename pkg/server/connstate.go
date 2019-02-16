package server

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/client"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/wrh"
)

type connState struct {
	srv  *Server
	conn net.Conn
	*protocol.Protocol
	bf *buffer.Buffer

	rCmd protocol.Cmd
}

const statsStr = `Key Count: %s
Keyspace size: %s
Dataspace size: %s
Requested Operation Count: %s
Successful Operation Count: %s
Slot Count: %s
`

const connectTimeout = 1 * time.Second
const connectRetryCount = 3
const pingTimeout = 1 * time.Second

func newConnState(srv *Server, conn net.Conn) (cs *connState) {
	cs = &connState{
		srv:      srv,
		conn:     conn,
		Protocol: protocol.New(conn, conn),
		bf:       buffer.New(),
	}
	return
}

func (cs *connState) getNode(addr string) (cl *client.Client) {
	cl = cs.srv.nodePool.Get("tcp", addr)
	if cl != nil {
		return
	}
	for i := 0; cl == nil && i < connectRetryCount; i++ {
		cl, _ = client.New("tcp", addr, connectTimeout, pingTimeout)
	}
	return
}

func (cs *connState) putNode(cl *client.Client) {
	cs.srv.nodePool.Put(cl)
}

func (cs *connState) cmdPing() (count int) {
	var err error
	err = cs.SendCmd(protocol.Cmd{Name: "PONG"})
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

const (
	serrCmdClusterArgCount = "invalid argument count"
)

func (cs *connState) cmdCluster() (count int) {
	var err error
	if len(cs.rCmd.Args) < 2 {
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{serrCmdClusterArgCount}})
		if err != nil {
			panic(err)
		}
		return
	}
	cs.srv.nodesMu.Lock()
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
	for _, addr := range cs.srv.nodeAddrs {
		for {
			cl := cs.srv.nodePool.Get("tcp", addr)
			if cl == nil {
				break
			}
			cl.Close()
		}
	}
	cs.srv.nodeAddrs = make(map[uint32]string, 4)
	for _, addr := range cs.srv.nodeAddrs2 {
		for {
			cl := cs.srv.nodePool.Get("tcp", addr)
			if cl == nil {
				break
			}
			cl.Close()
		}
	}
	cs.srv.nodeAddrs2 = make(map[uint32]string, 4)
	nodeCount := (len(cs.rCmd.Args) - 2) / 2
	cs.srv.nodes = make([]wrh.Node, 0, nodeCount)
	cs.srv.nodes2 = make([]wrh.Node, 0, nodeCount*2)
	for i := 0; i < nodeCount; i++ {
		idx := 2 + i*2
		var u uint64
		u, err = strconv.ParseUint(cs.rCmd.Args[idx+0], 10, 32)
		if err != nil {
			u = 0
		}
		id := uint32(u)
		addr := cs.rCmd.Args[idx+1]
		if _, ok := cs.srv.nodeAddrs[id]; ok {
			continue
		}
		nd := wrh.Node{
			Seed:   id,
			Weight: 1.0,
		}
		cs.srv.nodes = append(cs.srv.nodes, nd)
		cs.srv.nodes2 = append(cs.srv.nodes2, nd)
		cs.srv.nodeAddrs[id] = addr
		cs.srv.nodeAddrs2[id] = addr
	}
	args := make([]string, 0, nodeCount)
	for id, addr := range cs.srv.nodeAddrs {
		args = append(args, strconv.FormatUint(uint64(id), 10), addr)
	}
	cs.srv.clustered = true
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: args})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdNodeadd() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
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
		cs.srv.nodeAddrs2[id] = addr
		args = append(args, strconv.FormatUint(uint64(id), 10), addr)
		cs.srv.reshardNeed = true
	}
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: args})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdNoderm() (count int) {
	var err error
	cs.srv.nodesMu.Lock()
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
		delete(cs.srv.nodeAddrs2, id)
		args = append(args, strconv.FormatUint(uint64(id), 10))
		cs.srv.reshardNeed = true
	}
	cs.srv.nodesMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: args})
	if err != nil {
		panic(err)
	}
	return
}

const (
	serrCmdReshardDoesnotNeed   = "reshard does not need"
	serrCmdReshardCannotConnect = "can not connect to another node"
	serrCmdReshardCannotSet     = "can not set to another node"
)

func (cs *connState) cmdReshard() (count int) {
	cs.srv.reshardMu.Lock()
	var err error
	cs.srv.nodesMu.RLock()
	var serr []string
	if cs.srv.reshardNeed {
		bf := buffer.New()
		var val []byte
		respNodes := make([]wrh.Node, 1+cs.srv.nodeBackups)
		respNodes2 := make([]wrh.Node, 1+cs.srv.nodeBackups2)
		cs.srv.st.Scan(func(key string, size int, index int, data []byte, expiry int) (cont bool) {
			if index == 0 {
				val = bf.Want(size)
			}
			n := copy(val[index:], data)
			if index+n >= size {
				wrh.ResponsibleNodes(cs.srv.nodes, []byte(key), respNodes)
				wrh.ResponsibleNodes(cs.srv.nodes2, []byte(key), respNodes2)
				if wrh.MaxSeed(respNodes) == cs.srv.nodeID {
					for i, j := 0, len(respNodes2); i < j; i++ {
						id := respNodes2[i].Seed
						if id == cs.srv.nodeID {
							continue
						}
						addr := cs.srv.nodeAddrs2[id]
						cl := cs.getNode(addr)
						if cl == nil {
							serr = []string{serrCmdReshardCannotConnect, strconv.FormatUint(uint64(id), 10), addr}
							return false
						}
						k, _ := cl.Set([]string{key}, func(index int, key string) ([]byte, int) {
							return val, toExpires(expiry)
						})
						cs.putNode(cl)
						if len(k) < 1 {
							serr = []string{serrCmdReshardCannotSet, strconv.FormatUint(uint64(id), 10), addr}
							return false
						}
					}
				}
				if wrh.FindSeed(respNodes2, cs.srv.nodeID) < 0 {
					go cs.srv.st.Del(key)
				}
			}
			return true
		})
	} else {
		serr = []string{serrCmdReshardDoesnotNeed}
	}
	cs.srv.nodesMu.RUnlock()
	if serr != nil {
		cs.srv.reshardMu.Unlock()
		err = cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: serr})
		if err != nil {
			panic(err)
		}
		return
	}
	cs.srv.nodesMu.Lock()
	cs.srv.nodes = cs.srv.nodes2
	cs.srv.nodes2 = make([]wrh.Node, 0, len(cs.srv.nodes)*2)
	for i := range cs.srv.nodes {
		cs.srv.nodes2 = append(cs.srv.nodes2, cs.srv.nodes[i])
	}
	cs.srv.nodeAddrs = cs.srv.nodeAddrs2
	cs.srv.nodeAddrs2 = make(map[uint32]string, len(cs.srv.nodeAddrs)*2)
	for i := range cs.srv.nodeAddrs {
		cs.srv.nodeAddrs2[i] = cs.srv.nodeAddrs[i]
	}
	cs.srv.reshardNeed = false
	cs.srv.nodesMu.Unlock()
	cs.srv.reshardMu.Unlock()
	err = cs.SendCmd(protocol.Cmd{Name: "OK"})
	if err != nil {
		panic(err)
	}
	return
}

func (cs *connState) cmdGet() (count int) {
	var err error
	keys := make([]string, 0, len(cs.rCmd.Args))
	for _, key := range cs.rCmd.Args {
		if key == "" {
			continue
		}
		keys = append(keys, key)
	}
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: keys})
	if err != nil {
		panic(err)
	}
	for _, key := range keys {
		var buf []byte
		var expires int
		if cs.srv.st.Get(key, func(size int, index int, data []byte, expiry int) (cont bool) {
			if index == 0 {
				buf = cs.bf.Want(size)
				expires = toExpires(expiry)
			}
			copy(buf[index:], data)
			return
		}) {
			err = cs.SendData(buf, expires)
		} else {
			err = cs.SendData(nil, -1)
		}
		if err != nil {
			panic(err)
		}
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
	expiry := toExpiry(expires)
	key := cs.rCmd.Args[index]
	if key != "" {
		var val []byte
		f := func(size int, index int, data []byte, expiry int) (cont bool) {
			if !cs.srv.clustered {
				return false
			}
			if index == 0 {
				val = make([]byte, size)
			}
			copy(val[index:], data)
			/*if index+copy(val[index:], data) >= size {
			}*/
			return true
		}
		switch cs.rCmd.Name {
		case "SET":
			if !cs.srv.st.Set(key, data, expiry, f) {
				cs.rCmd.Args[index] = ""
			}
		case "PUT":
			if !cs.srv.st.Put(key, data, expiry, f) {
				cs.rCmd.Args[index] = ""
			}
		case "APPEND":
			if !cs.srv.st.Append(key, data, expiry, f) {
				cs.rCmd.Args[index] = ""
			}
		}
		//cs.srv.nodesMu.RLock()
		//cs.srv.nodesMu.RUnlock()
	}
	if index+1 >= count {
		keys := make([]string, 0, len(cs.rCmd.Args))
		for _, key := range cs.rCmd.Args {
			if key == "" {
				continue
			}
			keys = append(keys, key)
		}
		err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: keys})
		if err != nil {
			panic(err)
		}
	}
	return
}

func (cs *connState) cmdDel() (count int) {
	var err error
	keys := make([]string, 0, len(cs.rCmd.Args))
	for _, key := range cs.rCmd.Args {
		if key == "" {
			continue
		}
		if !cs.srv.st.Del(key) {
			continue
		}
		keys = append(keys, key)
	}
	err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: keys})
	if err != nil {
		panic(err)
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
	if cs.rCmd.Name == "STATS" {
		return cs.cmdStats()
	}
	if cs.rCmd.Name == "CLUSTER" {
		return cs.cmdCluster()
	}
	if cs.rCmd.Name == "NODEADD" {
		return cs.cmdNodeadd()
	}
	if cs.rCmd.Name == "NODERM" {
		return cs.cmdNoderm()
	}
	if cs.rCmd.Name == "RESHARD" {
		return cs.cmdReshard()
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
	panic(&protocol.Error{Err: ErrUnknownCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(count int, index int, data []byte, expires int) {
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
		cs.cmddataSet(count, index, data, expires)
		return
	}
}

func (cs *connState) OnQuit(e error) {
	defer func() {
		cs.bf.Close()
		cs.Flush()
	}()
	if e != nil && e != io.EOF {
		cmd := protocol.Cmd{Name: "FATAL"}
		if e, ok := e.(*protocol.Error); ok {
			cmd.Args = append(cmd.Args, e.Err.Error())
			if e.Err == ErrUnknownCommand {
				cmd.Args = append(cmd.Args, e.Cmd.Name)
			}
		}
		cs.SendCmd(cmd)
		return
	}
	cs.SendCmd(protocol.Cmd{Name: "QUIT"})
}
