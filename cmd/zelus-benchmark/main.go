package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
)

var wg sync.WaitGroup

func run(start, stop int) {
	keys := make([]string, stop-start)
	for i := start; i < stop; i++ {
		keys[i-start] = fmt.Sprintf("%d", i)
	}
	l := 16
	k1 := make([]string, 0, l)
	v1 := make([][]byte, 0, l)
	//cl, err := client.New("unix", "/tmp/zelus.sock")
	cl, err := client.New("tcp", "127.0.0.1:1234")
	if err != nil {
		panic(err)
	}
	var buf [4096]byte
	fmt.Println(time.Now(), "start")
	for i := start; i < stop; i += l {
		k1 = k1[:0]
		v1 = v1[:0]
		for j := 0; j < l; j++ {
			k1 = append(k1, keys[i+j-start])
			v1 = append(v1, buf[:])
		}
		k, _ := cl.Set(k1, v1)
		if len(k) != l {
			panic(i)
		}
	}
	fmt.Println(time.Now(), "end")
	ctx, cf := context.WithDeadline(context.Background(), time.Now().Add(1*time.Second))
	cl.Shutdown(ctx)
	cf()
	wg.Done()
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	for i := 0; i < 16; i++ {
		wg.Add(1)
		go run(i*512*1024*1024/4096, (i+1)*512*1024*1024/4096)
	}
	wg.Wait()
	time.Sleep(1 * time.Second)
}
