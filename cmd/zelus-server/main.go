package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/server"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

func main() {
	fmt.Printf("zelus-server\n\n")
	pp := os.Getenv("PPROF")
	if pp != "" {
		go func() {
			log.Fatalln(http.ListenAndServe(pp, nil))
		}()
	}
	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.Println("Started zelus-server")
	var err error
	ca := newCmdArgs(os.Stdout)
	err = ca.Parse(os.Args[1:])
	if err != nil {
		logger.Println("Error:", err.Error())
		os.Exit(2)
	}
	defer func() {
		if e, ok := recover().(error); ok {
			fmt.Println("Error:", e.Error())
			fmt.Println("")
			logger.Println("Error:", e.Error())
			os.Exit(1)
		}
		fmt.Println("")
	}()

	logger.Printf("Listening socket...\n")
	protocol.BufferSize = 1 * 1024 * 1024
	server.ConnBufferSize = 1 * 1024 * 1024
	var lst net.Listener
	var addr string
	if ca.Socket == "" {
		lst, err = net.Listen("tcp", ca.Bind)
		addr = ca.Bind
	} else {
		os.Remove(ca.Socket)
		lst, err = net.Listen("unix", ca.Socket)
		addr = ca.Socket
	}
	if err != nil {
		panic(err)
	}

	logger.Printf("Allocating %dMiB of memory...\n", ca.Capacity)
	size := int(ca.Capacity) * 1024 * 1024
	//count := size / (4 * malloc.MinLength)
	count := size / 4096
	st := store.New(count, size)
	defer st.Close()
	srv := server.New(st)

	logger.Printf("Accepting connections from %s", addr)
	sigIntCh := make(chan os.Signal, 1)
	signal.Notify(sigIntCh, os.Interrupt)
	go func() {
		time.Sleep(250 * time.Microsecond)
		<-sigIntCh
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err != nil {
			logger.Println("Error:", err.Error())
		}
	}()
	sigKillCh := make(chan os.Signal, 1)
	signal.Notify(sigKillCh, os.Kill)
	go func() {
		time.Sleep(250 * time.Microsecond)
		<-sigKillCh
		srv.Close()
	}()
	err = srv.Serve(lst)
	if err != nil {
		panic(err)
	}
}
