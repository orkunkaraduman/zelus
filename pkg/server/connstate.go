package server

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/client"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type connState struct {
	srv  *Server
	conn net.Conn
	*protocol.Protocol
	bf *buffer.Buffer

	rCmd protocol.Cmd
}

func newConnState(srv *Server, conn net.Conn) (cs *connState) {
	cs = &connState{
		srv:      srv,
		conn:     conn,
		Protocol: protocol.New(conn, conn),
		bf:       buffer.New(),
	}
	return
}

func (cs *connState) OnReadCmd(cmd protocol.Cmd) (count int) {
	var err error
	cs.rCmd = cmd
	if cs.rCmd.Name == "QUIT" || cs.rCmd.Name == "ERROR" {
		count = -1
		return
	}
	if cs.rCmd.Name == "PING" {
		err = cs.SendCmd(protocol.Cmd{Name: "PONG", Args: []string{strconv.Itoa(int(time.Now().Unix()))}})
		if err != nil {
			panic(err)
		}
		return
	}
	if cs.rCmd.Name == "STATS" {
		stats := cs.srv.st.Stats()
		statsStr := fmt.Sprintf(store.StoreStatsStr,
			stats.KeyCount,
			stats.KeyspaceSize,
			stats.DataspaceSize,
			stats.ReqOperCount,
			stats.SucOperCount,
		)
		err = cs.SendCmd(protocol.Cmd{Name: "STATS", Args: nil})
		if err != nil {
			panic(err)
		}
		err = cs.SendData([]byte(statsStr), -1)
		if err != nil {
			panic(err)
		}
		return
	}
	if cs.rCmd.Name == "GET" {
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
			buf := cs.bf.Want(0)
			var expiry2 int
			if cs.srv.st.Get(key, func(size int, index int, data []byte, expiry int) (cont bool) {
				if index == 0 {
					buf = cs.bf.Want(size)
					expiry2 = expiry
					if expiry2 < 0 {
						expiry2 = -1
					} else {
						expiry2 -= int(time.Now().Unix())
						if expiry2 < 0 {
							expiry2 = 0
						}
					}
				}
				copy(buf[index:], data)
				return
			}) {
				err = cs.SendData(buf, expiry2)
			} else {
				err = cs.SendData(nil, -1)
			}
			if err != nil {
				panic(err)
			}
		}
		return
	}
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
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
	if cs.rCmd.Name == "DEL" {
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
	if cs.rCmd.Name == "SLAVE" {
		kvc := make([]chan keyVal, 0, len(cs.rCmd.Args))
		addrCh := make(chan string, len(cs.rCmd.Args))
		var wg, wg2 sync.WaitGroup
		for _, addr := range cs.rCmd.Args {
			cl, _ := client.New("tcp", addr)
			if cl != nil && cs.srv.sh.Add(addr) {
				c := make(chan keyVal)
				kvc = append(kvc, c)
				wg.Add(1)
				go func(addr string, cl *client.Client, c chan keyVal) {
					ok := true
					for kv := range c {
						k, _ := cl.Set([]string{kv.Key}, func(index int, key string) (val []byte, expiry int) {
							return kv.Val, kv.Expiry
						})
						wg2.Done()
						if len(k) <= 0 || k[0] != kv.Key {
							cs.srv.sh.Remove(addr)
							ok = false
							break
						}
					}
					if ok {
						addrCh <- addr
					}
					wg.Done()
				}(addr, cl, c)
			}
		}
		go func() {
			wg.Wait()
			close(addrCh)
		}()
		bf := buffer.New()
		val := bf.Want(0)
		cs.srv.st.Scan(func(key string, size int, index int, data []byte, expiry int) (cont bool) {
			if index == 0 {
				val = bf.Want(size)
			}
			n := copy(val[index:], data)
			if index+n >= size {
				kv := keyVal{
					Key:    key,
					Val:    val,
					Expiry: expiry,
				}
				if kv.Expiry < 0 {
					kv.Expiry = -1
				} else {
					kv.Expiry -= int(time.Now().Unix())
					if kv.Expiry < 0 {
						kv.Expiry = 0
					}
				}
				for _, c := range kvc {
					wg2.Add(1)
					c <- kv
				}
				wg2.Wait()
			}
			return true
		})
		for _, c := range kvc {
			close(c)
		}
		addrs := make([]string, 0, len(cs.rCmd.Args))
		for addr := range addrCh {
			addrs = append(addrs, addr)
		}
		bf.Close()
		err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: addrs})
		if err != nil {
			panic(err)
		}
		return
	}
	panic(&protocol.Error{Err: ErrUnknownCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(count int, index int, data []byte, expiry int) {
	var err error
	var expiry2 int
	if expiry < 0 {
		expiry2 = -1
	} else {
		expiry2 += int(time.Now().Unix())
	}
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
		key := cs.rCmd.Args[index]
		if key != "" {
			val := []byte(nil)
			f := func(size int, index int, data []byte, expiry int) (cont bool) {
				if index == 0 {
					val = make([]byte, size)
				}
				n := copy(val[index:], data)
				if index+n >= size {
					kv := keyVal{
						Key:    key,
						Val:    val,
						Expiry: expiry,
					}
					if kv.Expiry < 0 {
						kv.Expiry = -1
					} else {
						kv.Expiry -= int(time.Now().Unix())
						if kv.Expiry < 0 {
							kv.Expiry = 0
						}
					}
					cs.srv.sh.C <- kv
				}
				return true
			}
			switch cs.rCmd.Name {
			case "SET":
				if !cs.srv.st.Set(key, data, expiry2, f) {
					cs.rCmd.Args[index] = ""
				}
			case "PUT":
				if !cs.srv.st.Put(key, data, expiry2, nil) {
					cs.rCmd.Args[index] = ""
				}
			case "APPEND":
				if !cs.srv.st.Append(key, data, expiry2, nil) {
					cs.rCmd.Args[index] = ""
				}
			}
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
}

func (cs *connState) OnQuit(e error) {
	defer func() {
		cs.bf.Close()
		cs.Flush()
	}()
	if e != nil {
		if e, ok := e.(*protocol.Error); ok {
			cmd := protocol.Cmd{Name: "ERROR", Args: []string{e.Err.Error()}}
			if e.Err == ErrUnknownCommand {
				cmd.Args = append(cmd.Args, e.Cmd.Name)
			}
			cs.SendCmd(cmd)
			return
		}
	}
	cs.SendCmd(protocol.Cmd{Name: "QUIT"})
}
