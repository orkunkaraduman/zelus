package server

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type connState struct {
	conn net.Conn
	*protocol.Protocol
	bf *buffer.Buffer

	st *store.Store

	rCmd protocol.Cmd
}

func newConnState(conn net.Conn) (cs *connState) {
	cs = &connState{
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
		stats := cs.st.Stats()
		statsStr := fmt.Sprintf(
			"Key Count: %d\nKeyspace size: %d\nDataspace size: %d\nRequested Operation Count: %d\nSuccessful Operation Count: %d\n",
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
			if cs.st.Get(key, func(size int, index int, data []byte, expiry int) {
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
			if !cs.st.Del(key) {
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
	panic(&protocol.Error{Err: ErrUnknownCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(count int, index int, data []byte, expiry int) {
	var err error
	if expiry < 0 {
		expiry = -1
	} else {
		expiry += int(time.Now().Unix())
	}
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
		key := cs.rCmd.Args[index]
		if key != "" {
			switch cs.rCmd.Name {
			case "SET":
				if !cs.st.Set(key, data, expiry) {
					cs.rCmd.Args[index] = ""
				}
			case "PUT":
				if !cs.st.Put(key, data, expiry) {
					cs.rCmd.Args[index] = ""
				}
			case "APPEND":
				if !cs.st.Append(key, data, expiry) {
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
