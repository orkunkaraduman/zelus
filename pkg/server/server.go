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
	sh *slaveHandler
}

var (
	ConnBufferSize = 0
)

func New(st *store.Store) (srv *Server) {
	srv = &Server{
		st: st,
		ac: &accepter.Accepter{
			ErrorLog: nil, //log.New(os.Stdout, "", log.LstdFlags),
		},
		sh: newSlaveHandler(),
	}
	srv.ac.Handler = accepter.HandlerFunc(srv.serve)
	srv.sh.Inc()
	srv.sh.Inc()
	srv.sh.Inc()
	srv.sh.Inc()
	return
}

func (srv *Server) Serve(l net.Listener) error {
	return srv.ac.Serve(l)
}

func (srv *Server) TCPListenAndServe(addr string) error {
	return srv.ac.TCPListenAndServe(addr)
}

func (srv *Server) Shutdown(ctx context.Context) error {
	srv.sh.Close()
	return srv.ac.Shutdown(ctx)
}

func (srv *Server) Close() error {
	srv.sh.Close()
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
	//srv.sh.Inc()
	cs.Serve(cs, closeCh)
	//srv.sh.Dec()
}
