package client

import (
	"net"
	"sync"
)

type Client struct {
	mu   sync.Mutex
	conn net.Conn
	cs   *connState
}

func New(network, address string) (cl *Client, err error) {
	var conn net.Conn
	conn, err = net.Dial(network, address)
	if err != nil {
		return
	}
	cl = &Client{
		conn: conn,
		cs:   newConnState(conn),
	}
	go func() {
		cl.cs.Serve(cl.cs, nil)
		conn.Close()
	}()
	return
}

func (cl *Client) Close() {
	cl.cs.Close()
}

func (cl *Client) Get(keys []string) (k []string, v [][]byte, err error) {
	cl.mu.Lock()
	k, v, err = cl.cs.Get(keys)
	cl.mu.Unlock()
	return
}

func (cl *Client) Set(keys []string, vals [][]byte) (k []string, err error) {
	cl.mu.Lock()
	k, err = cl.cs.Set(keys, vals)
	cl.mu.Unlock()
	return
}
