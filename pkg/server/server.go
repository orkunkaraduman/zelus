package server

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

	accepter "github.com/orkunkaraduman/go-accepter"
	"github.com/orkunkaraduman/zelus/pkg/client"
	"github.com/orkunkaraduman/zelus/pkg/store"
	"github.com/orkunkaraduman/zelus/pkg/wrh"
)

type Server struct {
	st *store.Store
	ac *accepter.Accepter

	nodePool     *client.Pool
	nodesMu      sync.RWMutex
	clustered    bool
	nodeID       uint32
	nodeBackups  uint
	nodeBackups2 uint
	nodes        []wrh.Node
	nodes2       []wrh.Node
	nodeAddrs    map[uint32]string
	nodeAddrs2   map[uint32]string
	reshardNeed  bool
	reshardMu    sync.Mutex
}

var (
	ConnBufferSize = 0
)

func New(st *store.Store) (srv *Server) {
	srv = &Server{
		st: st,
		ac: &accepter.Accepter{
			ErrorLog: log.New(os.Stderr, "", log.LstdFlags),
		},
		nodePool: client.NewPool(1024),
	}
	srv.ac.Handler = accepter.HandlerFunc(srv.serve)
	return
}

func (srv *Server) Serve(l net.Listener) error {
	return srv.ac.Serve(l)
}

func (srv *Server) TCPListenAndServe(addr string) error {
	return srv.ac.TCPListenAndServe(addr)
}

func (srv *Server) Shutdown(ctx context.Context) error {
	srv.nodePool.Close()
	return srv.ac.Shutdown(ctx)
}

func (srv *Server) Close() error {
	srv.nodePool.Close()
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
