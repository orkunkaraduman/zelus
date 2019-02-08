package client

import (
	"context"
	"net"
	"runtime"
	"time"
)

type Client struct {
	conn net.Conn
	cs   *connState
}

type GetFunc func(index int, key string, val []byte, expiry int)
type SetFunc func(index int, key string) (val []byte, expiry int)

var (
	ConnBufferSize = 0
)

func New(network, address string) (cl *Client, err error) {
	var conn net.Conn
	conn, err = net.Dial(network, address)
	if err != nil {
		return
	}
	if ConnBufferSize > 0 {
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetReadBuffer(ConnBufferSize)
			tcpConn.SetWriteBuffer(ConnBufferSize)
		}
		if unixConn, ok := conn.(*net.UnixConn); ok {
			unixConn.SetReadBuffer(ConnBufferSize)
			unixConn.SetWriteBuffer(ConnBufferSize)
		}
	}
	cl = &Client{
		conn: conn,
		cs:   newConnState(conn),
	}
	return
}

func (cl *Client) Shutdown(ctx context.Context) (err error) {
	go cl.cs.Close(nil)
	runtime.Gosched()
	for {
		select {
		case <-time.After(5 * time.Millisecond):
			if cl.cs.IsClosed() {
				err = cl.conn.Close()
				return
			}
		case <-ctx.Done():
			cl.conn.Close()
			err = ctx.Err()
			for !cl.cs.IsClosed() {
				time.Sleep(5 * time.Millisecond)
			}
			return
		}
	}
}

func (cl *Client) Close() (err error) {
	err = cl.conn.Close()
	cl.cs.Close(nil)
	return
}

func (cl *Client) IsClosed() bool {
	return cl.cs.IsClosed()
}

func (cl *Client) Get(keys []string, f GetFunc) (err error) {
	return cl.cs.Get(keys, f)
}

func (cl *Client) Set(keys []string, f SetFunc) (k []string, err error) {
	return cl.cs.Set(keys, f)
}

func (cl *Client) Put(keys []string, f SetFunc) (k []string, err error) {
	return cl.cs.Put(keys, f)
}

func (cl *Client) Append(keys []string, f SetFunc) (k []string, err error) {
	return cl.cs.Append(keys, f)
}

func (cl *Client) Del(keys []string) (k []string, err error) {
	return cl.cs.Del(keys)
}
