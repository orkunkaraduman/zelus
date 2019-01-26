package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
	"github.com/orkunkaraduman/zelus/pkg/protocol"
)

func main() {
	fmt.Printf("zelus-benchmark\n\n")
	pp := os.Getenv("PPROF")
	if pp != "" {
		go func() {
			log.Fatalln(http.ListenAndServe(pp, nil))
		}()
	}
	var err error
	ca := newCmdArgs(os.Stdout)
	err = ca.Parse(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}

	defer func() {
		if e, ok := recover().(error); ok {
			fmt.Println("Error:", e.Error())
			fmt.Println("")
			os.Exit(1)
		}
		fmt.Println("")
	}()

	fmt.Printf("Connecting clients to server...\n")
	protocol.BufferSize = 1 * 1024 * 1024
	client.ConnBufferSize = 1 * 1024 * 1024
	cls := make([]*client.Client, ca.Clients)
	closeFunc := func() {
		for _, cl := range cls {
			if cl == nil {
				continue
			}
			cl.Close()
		}
	}
	defer closeFunc()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		closeFunc()
	}()
	for i := range cls {
		if ca.Socket == "" {
			cls[i], err = client.New("tcp", ca.Hostname)
		} else {
			cls[i], err = client.New("unix", ca.Socket)
		}
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("Preparing...\n\n")
	brChs := make([]chan benchmarkResult, ca.Clients)
	for i := range brChs {
		brChs[i] = make(chan benchmarkResult)
	}
	keys := make([]string, ca.Requests)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", ca.Prefix, i)
	}
	fmt.Printf("Number of clients:  %d\nNumber of requests: %d\n\n\n", ca.Clients, ca.Requests)

	numberOfRequests := int(ca.Requests)
	requestsPerClient := int(ca.Requests / ca.Clients)
	tests := strings.Split(strings.ToUpper(ca.Tests), ",")
	err = nil
	for err == nil {
		for _, test := range tests {
			test = strings.TrimSpace(test)
			count := int64(0)
			n, m := 0, numberOfRequests-requestsPerClient*len(cls)
			for i := range cls {
				l := requestsPerClient
				if m > 0 {
					l++
					m--
				}
				switch test {
				case "SET":
					go set(cls[i], keys[n:n+l], int(ca.Multi), int(ca.Datasize), brChs[i], &count)
				default:
					panic(fmt.Errorf("test %s is unknown", test))
				}
				n += l
			}
			startTm := time.Now()
			lastCount := int64(0)
			lastNs := startTm.UnixNano()
			tk := time.NewTicker(1 * time.Second)
			go func() {
				for now := range tk.C {
					ns := now.UnixNano()
					currCount := atomic.LoadInt64(&count)
					fmt.Printf("%12d/%d: %d req/s\n",
						count,
						len(keys),
						1000000000*(currCount-lastCount)/(ns-lastNs),
					)
					lastCount = currCount
					lastNs = ns
				}
			}()
			fmt.Printf("Test %s started.\n", test)
			tbr := benchmarkResult{}
			for _, brCh := range brChs {
				br := <-brCh
				if err == nil {
					err = br.err
				}
				tbr.count += br.count
				tbr.duration += br.duration
			}
			tk.Stop()
			runtime.Gosched()
			ns := tbr.duration.Nanoseconds() / int64(len(cls))
			fmt.Printf("\n Number of requests:  %d\n Avarage duration:    %v\n Requests per second: %d\n\n\n",
				tbr.count,
				time.Duration(ns),
				1000000000*tbr.count/ns,
			)
			if err != nil {
				break
			}
		}
		if !ca.Loop {
			break
		}
	}
}
