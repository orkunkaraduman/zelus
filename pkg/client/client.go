package client

import (
	"context"
	"net"
	"time"
)

type Client struct {
	conn net.Conn
	cs   *connState
}

type GetFunc func(key string, val []byte)

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
			return
		}
	}
}

func (cl *Client) Close() (err error) {
	err = cl.conn.Close()
	cl.cs.Close(nil)
	return
}

func (cl *Client) Get(keys []string, f GetFunc) (err error) {
	return cl.cs.Get(keys, f)
}

func (cl *Client) Set(keys []string, vals [][]byte) (k []string, err error) {
	return cl.cs.Set(keys, vals)
}
