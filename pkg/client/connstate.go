package client

import (
	"net"
	"runtime"
	"sync"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
)

type connState struct {
	conn net.Conn
	*protocol.Protocol
	bf *buffer.Buffer

	mu     sync.Mutex
	closed bool

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
		if cs.sCmd.Name == "SET" || cs.sCmd.Name == "PUT" || cs.sCmd.Name == "APPEND" {
			return
		}
		if cs.sCmd.Name == "DEL" {
			return
		}
	}
	panic(&protocol.Error{Err: ErrUnknownCommand, Cmd: cs.rCmd})
}

func (cs *connState) OnReadData(count int, index int, data []byte, expiry int) {
	if cs.rCmd.Name == "OK" {
		if cs.sCmd.Name == "GET" {
			cs.gf(index, cs.rCmd.Args[index], data, expiry)
			return
		}
	}
}

func (cs *connState) OnQuit(e error) {
	if e != nil {
		go cs.Close(e)
		runtime.Gosched()
	}
}

func (cs *connState) Close(e error) {
	cs.mu.Lock()
	if cs.closed {
		cs.mu.Unlock()
		return
	}
	defer func() {
		cs.bf.Close()
		cs.Flush()
		cs.conn.Close()
		cs.closed = true
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

func (cs *connState) IsClosed() bool {
	cs.mu.Lock()
	r := cs.closed
	cs.mu.Unlock()
	return r
}

func (cs *connState) Get(keys []string, f GetFunc) (err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.closed {
		panic(nil)
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
	if !cs.Receive(cs, cs.bf) {
		panic(nil)
	}
	return
}

func (cs *connState) Set(keys []string, f SetFunc) (k []string, err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.closed {
		panic(nil)
	}
	cmd := protocol.Cmd{Name: "SET", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		panic(err)
	}
	for index, key := range keys {
		val, expiry := f(index, key)
		err = cs.SendData(val, expiry)
		if err != nil {
			panic(err)
		}
	}
	err = cs.Flush()
	if err != nil {
		panic(err)
	}
	cs.sCmd = cmd
	if !cs.Receive(cs, cs.bf) {
		panic(nil)
	}
	k = make([]string, 0, len(keys))
	for _, key := range cs.rCmd.Args {
		k = append(k, key)
	}
	return
}

func (cs *connState) Put(keys []string, f SetFunc) (k []string, err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.closed {
		panic(nil)
	}
	cmd := protocol.Cmd{Name: "PUT", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		panic(err)
	}
	for index, key := range keys {
		val, expiry := f(index, key)
		err = cs.SendData(val, expiry)
		if err != nil {
			panic(err)
		}
	}
	err = cs.Flush()
	if err != nil {
		panic(err)
	}
	cs.sCmd = cmd
	if !cs.Receive(cs, cs.bf) {
		panic(nil)
	}
	k = make([]string, 0, len(keys))
	for _, key := range cs.rCmd.Args {
		k = append(k, key)
	}
	return
}

func (cs *connState) Append(keys []string, f SetFunc) (k []string, err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.closed {
		panic(nil)
	}
	cmd := protocol.Cmd{Name: "APPEND", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		panic(err)
	}
	for index, key := range keys {
		val, expiry := f(index, key)
		err = cs.SendData(val, expiry)
		if err != nil {
			panic(err)
		}
	}
	err = cs.Flush()
	if err != nil {
		panic(err)
	}
	cs.sCmd = cmd
	if !cs.Receive(cs, cs.bf) {
		panic(nil)
	}
	k = make([]string, 0, len(keys))
	for _, key := range cs.rCmd.Args {
		k = append(k, key)
	}
	return
}

func (cs *connState) Del(keys []string) (k []string, err error) {
	cs.mu.Lock()
	defer func() {
		err, _ = recover().(error)
		cs.OnQuit(err)
		cs.mu.Unlock()
	}()
	if cs.closed {
		panic(nil)
	}
	cmd := protocol.Cmd{Name: "DEL", Args: keys}
	err = cs.SendCmd(cmd)
	if err != nil {
		panic(err)
	}
	err = cs.Flush()
	if err != nil {
		panic(err)
	}
	cs.sCmd = cmd
	if !cs.Receive(cs, cs.bf) {
		panic(nil)
	}
	k = make([]string, 0, len(keys))
	for _, key := range cs.rCmd.Args {
		k = append(k, key)
	}
	return
}
