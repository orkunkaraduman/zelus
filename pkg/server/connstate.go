package server

import (
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
		err = cs.SendCmd(protocol.Cmd{Name: "PONG", Args: []string{strconv.Itoa(int(time.Now().UnixNano() / 1000))}})
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
			if cs.st.Get(key, func(size int, index int, data []byte) {
				if index == 0 {
					buf = cs.bf.Want(size)
				}
				copy(buf[index:], data)
			}) {
				err = cs.SendData(buf)
			} else {
				err = cs.SendData(nil)
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

func (cs *connState) OnReadData(count int, index int, data []byte) {
	var err error
	if cs.rCmd.Name == "SET" || cs.rCmd.Name == "PUT" || cs.rCmd.Name == "APPEND" {
		key := cs.rCmd.Args[index]
		if key != "" {
			switch cs.rCmd.Name {
			case "SET":
				if !cs.st.Set(key, data, 0) {
					cs.rCmd.Args[index] = ""
				}
			case "PUT":
				if !cs.st.Put(key, data, 0) {
					cs.rCmd.Args[index] = ""
				}
			case "APPEND":
				if !cs.st.Append(key, data) {
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
