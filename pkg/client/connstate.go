package client

import (
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
)

type connState struct {
	conn net.Conn
	*protocol.Protocol
	bf *buffer.Buffer

	mu   sync.Mutex
	done int32

	rCmd protocol.Cmd
	sCmd protocol.Cmd
	gf   GetFunc
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
	cs.rCmd = cmd
	if cs.rCmd.Name == "QUIT" || cs.rCmd.Name == "ERROR" {
		count = -1
		return
	}
	if cs.rCmd.Name == "OK" {
		if cs.sCmd.Name == "GET" {
			count = len(cs.rCmd.Args)
			return
		}
		if cs.sCmd.Name == "SET" {
			return
		}
	}
	panic(&protocol.Error{Err: ErrUnknownCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(count int, index int, data []byte) {
	if cs.rCmd.Name == "OK" {
		if cs.sCmd.Name == "GET" {
			cs.gf(cs.rCmd.Args[index], data)
			return
		}
	}
}

func (cs *connState) OnQuit(e error) {
	if e != nil {
		go cs.Close(e)
	}
}

func (cs *connState) Done() bool {
	return atomic.LoadInt32(&cs.done) != 0
}

func (cs *connState) Close(e error) {
	if cs.Done() {
		return
	}
	cs.mu.Lock()
	defer func() {
		cs.bf.Close()
		cs.Flush()
		cs.conn.Close()
		atomic.CompareAndSwapInt32(&cs.done, 0, 1)
		cs.mu.Unlock()
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

func (cs *connState) Get(keys []string, f GetFunc) (err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.Done() {
		panic(io.EOF)
	}
	cmd := protocol.Cmd{Name: "GET", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		panic(err)
	}
	err = cs.Flush()
	if err != nil {
		panic(err)
	}
	cs.sCmd = cmd
	cs.gf = f
	cs.Receive(cs, cs.bf)
	return
}

func (cs *connState) Set(keys []string, vals [][]byte) (k []string, err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.Done() {
		panic(io.EOF)
	}
	cmd := protocol.Cmd{Name: "SET", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		panic(err)
	}
	for i := range keys {
		val := vals[i]
		err = cs.SendData(val)
		if err != nil {
			panic(err)
		}
	}
	err = cs.Flush()
	if err != nil {
		panic(err)
	}
	cs.sCmd = cmd
	cs.Receive(cs, cs.bf)
	k = make([]string, 0, len(keys))
	for _, key := range cs.rCmd.Args {
		k = append(k, key)
	}
	return
}
