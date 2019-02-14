package client

import (
	"context"
	"net"
	"time"
)

type Client struct {
	network, address string
	conn             net.Conn
	cs               *connState
}

type GetFunc func(index int, key string, val []byte, expiry int)
type SetFunc func(index int, key string) (val []byte, expiry int)

var (
	ConnBufferSize = 0
)

func New(network, address string, timeout time.Duration) (cl *Client, err error) {
	var conn net.Conn
	d := net.Dialer{
		Timeout:   timeout,
		KeepAlive: 65 * time.Second,
	}
	conn, err = d.Dial(network, address)
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
		network: network,
		address: address,
		conn:    conn,
		cs:      newConnState(conn),
	}
	return
}

func (cl *Client) Shutdown(ctx context.Context) (err error) {
	go cl.cs.Close(nil)
	for {
		select {
		case <-time.After(5 * time.Millisecond):
			if cl.cs.IsClosed() {
				return
			}
		case <-ctx.Done():
			cl.Close()
			err = ctx.Err()
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

func (cl *Client) Ping(ctx context.Context) (err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	e := error(nil)
	c := make(chan struct{})
	go func() {
		e = cl.cs.Ping()
		c <- struct{}{}
	}()
	select {
	case <-c:
		err = e
	case <-ctx.Done():
		cl.Close()
		err = ctx.Err()
	}
	return
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
