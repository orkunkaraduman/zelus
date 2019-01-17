package client

import (
	"net"
	"sync/atomic"

	"github.com/orkunkaraduman/zelus/pkg/protocol"
)

type connState struct {
	*protocol.Protocol
	parser *protocol.CmdParser
	done   int32

	rCmd   protocol.Cmd
	rIndex int
	rCount int

	sCmdQueue chan protocol.Cmd
	sCmd      protocol.Cmd

	resultQueue chan *keyVal
}

func newConnState(conn net.Conn) (cs *connState) {
	cs = &connState{
		Protocol: protocol.New(conn, conn),
		parser:   protocol.NewCmdParser(),
	}
	cs.sCmdQueue = make(chan protocol.Cmd)
	cs.resultQueue = make(chan *keyVal)
	return
}

func (cs *connState) OnReadCmd(cmd protocol.Cmd) (count int) {
	cs.rCmd = cmd
	cs.rIndex = 0
	cs.rCount = 0
	if cs.rCmd.Name == "QUIT" || cs.rCmd.Name == "ERROR" {
		count = -1
		return
	}
	var ok bool
	cs.sCmd, ok = <-cs.sCmdQueue
	if !ok {
		count = -1
		return
	}
	if cs.rCmd.Name == "OK" {
		if cs.sCmd.Name == "GET" {
			count = len(cs.rCmd.Args)
			if count > 0 {
				cs.rCount = count
				return
			}
			cs.resultQueue <- nil
			return
		}
		if cs.sCmd.Name == "SET" {
			for _, arg := range cs.rCmd.Args {
				cs.resultQueue <- &keyVal{Key: arg}
			}
			cs.resultQueue <- nil
			return
		}
	}
	panic(&protocol.Error{Err: ErrUnknownCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(data []byte) {
	index := cs.rIndex
	cs.rIndex++
	if cs.rCmd.Name == "OK" {
		if cs.sCmd.Name == "GET" {
			cs.resultQueue <- &keyVal{Key: cs.rCmd.Args[index], Val: data}
			if cs.rIndex >= cs.rCount {
				cs.resultQueue <- nil
			}
			return
		}
	}
}

func (cs *connState) OnQuit(e error) {
	close(cs.resultQueue)
	if e != nil {
		if e, ok := e.(*protocol.Error); ok {
			cmd := protocol.Cmd{Name: "ERROR", Args: []string{e.Err.Error()}}
			if e.Err == ErrUnknownCommand {
				cmd.Args = append(cmd.Args, e.Cmd.Name)
			}
			cs.SendCmd(cmd)
			cs.Flush()
			atomic.CompareAndSwapInt32(&cs.done, 0, 1)
			return
		}
	}
	cs.SendCmd(protocol.Cmd{Name: "QUIT"})
	cs.Flush()
	atomic.CompareAndSwapInt32(&cs.done, 0, 1)
}

func (cs *connState) Done() bool {
	return atomic.LoadInt32(&cs.done) != 0
}

func (cs *connState) Close() {
	var ok bool
	select {
	case _, ok = <-cs.sCmdQueue:
	default:
		ok = true
	}
	if ok {
		close(cs.sCmdQueue)
	}
}

func (cs *connState) Get(keys []string) (k []string, v [][]byte, err error) {
	cmd := protocol.Cmd{Name: "GET", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		return
	}
	err = cs.Flush()
	if err != nil {
		return
	}
	func() {
		defer func() { recover() }()
		cs.sCmdQueue <- cmd
		k = make([]string, 0, len(keys))
		v = make([][]byte, 0, len(keys))
	}()
	if v == nil {
		return
	}
	for kv := range cs.resultQueue {
		if kv == nil {
			break
		}
		k = append(k, kv.Key)
		v = append(v, kv.Val)
	}
	return
}

func (cs *connState) Set(keys []string, vals [][]byte) (k []string, err error) {
	cmd := protocol.Cmd{Name: "SET", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		return
	}
	for i, _ := range keys {
		val := vals[i]
		err = cs.SendData(val)
		if err != nil {
			return
		}
	}
	err = cs.Flush()
	if err != nil {
		return
	}
	func() {
		defer func() { recover() }()
		cs.sCmdQueue <- cmd
		k = make([]string, 0, len(keys))
	}()
	if k == nil {
		return
	}
	for kv := range cs.resultQueue {
		if kv == nil {
			break
		}
		k = append(k, kv.Key)
	}
	return
}
