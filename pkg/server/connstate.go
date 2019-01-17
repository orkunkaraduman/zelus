package server

import (
	"net"

	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type connState struct {
	*protocol.Protocol
	parser *protocol.CmdParser

	st *store.Store

	rCmd   protocol.Cmd
	rIndex int
	rCount int
}

func newConnState(conn net.Conn) (cs *connState) {
	cs = &connState{
		Protocol: protocol.New(conn, conn),
		parser:   protocol.NewCmdParser(),
	}
	return
}

func (cs *connState) OnReadCmd(cmd protocol.Cmd) (count int) {
	var err error
	cs.rCmd = cmd
	cs.rIndex = 0
	cs.rCount = 0
	if cs.rCmd.Name == "QUIT" || cs.rCmd.Name == "ERROR" {
		cs.Close()
		return
	}
	if cs.rCmd.Name == "GET" {
		keys := make([]string, 0, len(cs.rCmd.Args))
		vals := make([][]byte, 0, len(cs.rCmd.Args))
		var buf [32 * 1024]byte
		for _, key := range cs.rCmd.Args {
			if key == "" {
				continue
			}
			b := []byte(nil)
			if len(keys) == 0 {
				b = buf[:]
			}
			val := cs.st.Get(key, b)
			if val != nil {
				keys = append(keys, key)
				vals = append(vals, val)
			}
		}
		err = cs.SendCmd(protocol.Cmd{Name: "OK", Args: keys})
		if err != nil {
			panic(err)
		}
		for _, val := range vals {
			err = cs.SendData(val)
			if err != nil {
				panic(err)
			}
		}
		return
	}
	if cs.rCmd.Name == "SET" {
		count = len(cs.rCmd.Args)
		if count > 0 {
			cs.rCount = count
			return
		}
		err = cs.SendCmd(protocol.Cmd{Name: "OK"})
		if err != nil {
			panic(err)
		}
		return
	}
	return
}

func (cs *connState) OnReadData(data []byte) {
	var err error
	index := cs.rIndex
	cs.rIndex++
	if cs.rCmd.Name == "SET" {
		key := cs.rCmd.Args[index]
		if key != "" {
			if !cs.st.Set(key, data, true) {
				cs.rCmd.Args[index] = ""
			}
		}
		if cs.rIndex >= cs.rCount {
			var keys []string
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
	if e != nil {
		if e, ok := e.(*protocol.Error); ok {
			cs.SendCmd(protocol.Cmd{Name: "ERROR", Args: []string{e.Err.Error()}})
			cs.Flush()
		}
		return
	}
	cs.SendCmd(protocol.Cmd{Name: "QUIT"})
	cs.Flush()
}
