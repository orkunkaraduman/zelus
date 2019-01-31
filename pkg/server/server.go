package server

import (
	"context"
	"net"

	accepter "github.com/orkunkaraduman/go-accepter"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type Server struct {
	st *store.Store
	ac *accepter.Accepter
}

var (
	ConnBufferSize = 0
)

func New(st *store.Store) (srv *Server) {
	srv = &Server{
		st: st,
		ac: &accepter.Accepter{
			Handler:  accepter.HandlerFunc(srv.serve),
			ErrorLog: nil, //log.New(os.Stdout, "", log.LstdFlags),
		},
	}
	return
}

func (srv *Server) Serve(l net.Listener) error {
	return srv.ac.Serve(l)
}

func (srv *Server) TCPListenAndServe(addr string) error {
	return srv.ac.TCPListenAndServe(addr)
}

func (srv *Server) Shutdown(ctx context.Context) error {
	return srv.ac.Shutdown(ctx)
}

func (srv *Server) Close() error {
	return srv.ac.Close()
}

func (srv *Server) serve(conn net.Conn, closeCh <-chan struct{}) {
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
	cs := newConnState(srv, conn)
	cs.Serve(cs, closeCh)
}
