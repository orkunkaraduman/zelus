package server

import (
	"context"
	"log"
	"net"
	"os"

	. "github.com/orkunkaraduman/go-accepter"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

type Server struct {
	st *store.Store
	ac *Accepter
}

func New(st *store.Store) (srv *Server) {
	srv = &Server{
		st: st,
	}
	srv.ac = &Accepter{
		Handler:  HandlerFunc(srv.serve),
		ErrorLog: log.New(os.Stdout, "", log.LstdFlags),
	}
	return
}

func (srv *Server) ListenAndServe(addr string) error {
	return srv.ac.TCPListenAndServe(addr)
}

func (srv *Server) Shutdown(ctx context.Context) error {
	return srv.ac.Shutdown(ctx)
}

func (srv *Server) serve(conn net.Conn, closeCh <-chan struct{}) {
	cn := newClientConn(conn)
	cn.st = srv.st
	cn.Serve(cn, closeCh)
}
