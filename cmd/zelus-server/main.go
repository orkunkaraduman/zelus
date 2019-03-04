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
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/malloc"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
	"github.com/orkunkaraduman/zelus/pkg/server"
	"github.com/orkunkaraduman/zelus/pkg/store"
)

func main() {
	debug.SetGCPercent(5)
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
	memPool := malloc.AllocPool(int(ca.Capacity) * 1024 * 1024)
	//memPool := &nativeMemPool{}
	//mPool := store.MemPool(nil)
	//store.NativeAlloc = true
	st := store.New(64*1024, 4, memPool, memPool)
	srv := server.New(st)

	logger.Printf("Accepting connections from %s\n", addr)
	sigIntCh := make(chan os.Signal, 1)
	signal.Notify(sigIntCh, os.Interrupt)
	sigTermCh := make(chan os.Signal, 1)
	signal.Notify(sigTermCh, syscall.SIGTERM)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		time.Sleep(250 * time.Microsecond)
		select {
		case <-sigIntCh:
		case <-sigTermCh:
		}
		logger.Printf("Shutting down...\n")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := srv.Shutdown(ctx)
		if err != nil {
			logger.Println("Error:", err.Error())
		}
		wg.Done()
	}()
	err = srv.Serve(lst)
	if err != nil {
		panic(err)
	}
	wg.Wait()
	st.Close()
	logger.Printf("Finished zelus-server\n")
}
