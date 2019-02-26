package server

import (
	"context"
	"log"
	"net"
	"os"
	"sync"

	accepter "github.com/orkunkaraduman/go-accepter"

	"github.com/orkunkaraduman/zelus/pkg/buffer"
	"github.com/orkunkaraduman/zelus/pkg/store"
	"github.com/orkunkaraduman/zelus/pkg/wrh"
)

type Server struct {
	st *store.Store
	ac *accepter.Accepter

	nodesMu         sync.RWMutex
	clusterState    int
	nodeID          uint32
	nodeBackups     uint
	nodeBackups2    uint
	nodes           []wrh.Node
	nodes2          []wrh.Node
	nodeQueueGroups map[uint32]*queueGroup
	bfPool          *buffer.Pool
}

const (
	clusterStateNonclustered = iota
	clusterStateNormal
	clusterStateReshardWait
	clusterStateReshard
	clusterStateCleanWait
	clusterStateClean
)

const (
	maxKeyCount  = 128
	maxRespNodes = 32
)

var (
	ConnBufferSize = 0
)

func New(st *store.Store) (srv *Server) {
	srv = &Server{
		st: st,
		ac: &accepter.Accepter{
			ErrorLog: log.New(os.Stderr, "", log.LstdFlags),
		},
		bfPool: buffer.NewPool(maxKeyCount * maxRespNodes),
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
