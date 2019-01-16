package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/orkunkaraduman/zelus/pkg/server"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	size := 10 * 1024 * 1024 * 1024
	count := 2 * size / 4096
	st := store.New(count, size)
	srv := server.New(st)
	//srv.TCPListenAndServe("0.0.0.0:1234")
	//lst, err := net.Listen("unix", "/tmp/zelus.sock")
	lst, err := net.Listen("tcp", "127.0.0.1:1234")
	if err != nil {
		panic(err)
	}
	fmt.Println("ready")
	srv.Serve(lst)
}
