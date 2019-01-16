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
	/*if tcpConn, ok := conn.(*net.TCPConn); ok {
		var err error
		err = tcpConn.SetReadBuffer(1 * 1024 * 1024)
		if err != nil {
			panic(err)
		}
		err = tcpConn.SetWriteBuffer(1 * 1024 * 1024)
		if err != nil {
			panic(err)
		}
	}*/
	cs := newConnState(conn)
	cs.st = srv.st
	cs.Serve(cs, closeCh)
}
