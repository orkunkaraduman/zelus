package client

import (
	"context"
	"net"
	"sync"
	"time"
)

type Client struct {
	mu      sync.Mutex
	conn    net.Conn
	cs      *connState
	closeCh chan struct{}
}

var (
	ConnBuffer = 0
)

func New(network, address string) (cl *Client, err error) {
	var conn net.Conn
	conn, err = net.Dial(network, address)
	if err != nil {
		return
	}
	if ConnBuffer > 0 {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetReadBuffer(ConnBuffer)
			tcpConn.SetWriteBuffer(ConnBuffer)
		}
		if unixConn, ok := conn.(*net.UnixConn); ok {
			unixConn.SetReadBuffer(ConnBuffer)
			unixConn.SetWriteBuffer(ConnBuffer)
		}
	}
	cl = &Client{
		conn:    conn,
		cs:      newConnState(conn),
		closeCh: make(chan struct{}, 1),
	}
	go cl.cs.Serve(cl.cs, cl.closeCh)
	return
}

func (cl *Client) Shutdown(ctx context.Context) (err error) {
	select {
	case cl.closeCh <- struct{}{}:
	default:
	}
	if tcpConn, ok := cl.conn.(*net.TCPConn); ok {
		tcpConn.CloseRead()
	}
	if unixConn, ok := cl.conn.(*net.UnixConn); ok {
		unixConn.CloseRead()
	}
	cl.cs.Close()
	for {
		select {
		case <-time.After(5 * time.Millisecond):
			if cl.cs.Done() {
				err = cl.conn.Close()
				return
			}
		case <-ctx.Done():
			cl.conn.Close()
			err = ctx.Err()
			return
		}
	}
}

func (cl *Client) Close() (err error) {
	select {
	case cl.closeCh <- struct{}{}:
	default:
	}
	if tcpConn, ok := cl.conn.(*net.TCPConn); ok {
		tcpConn.CloseRead()
	}
	if unixConn, ok := cl.conn.(*net.UnixConn); ok {
		unixConn.CloseRead()
	}
	cl.cs.Close()
	err = cl.conn.Close()
	return
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
