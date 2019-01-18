package server

import (
	"context"
	"log"
	"net"
	"os"

	accepter "github.com/orkunkaraduman/go-accepter"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type Server struct {
	st *store.Store
	ac *accepter.Accepter
}

var (
	ConnBuffer = 0
)

func New(st *store.Store) (srv *Server) {
	srv = &Server{
		st: st,
	}
	srv.ac = &accepter.Accepter{
		Handler:  accepter.HandlerFunc(srv.serve),
		ErrorLog: log.New(os.Stdout, "", log.LstdFlags),
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

func (srv *Server) serve(conn net.Conn, closeCh <-chan struct{}) {
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
	cs := newConnState(conn)
	cs.st = srv.st
	cs.Serve(cs, closeCh)
}
