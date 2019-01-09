// +build ignore

package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/store"
)

var wg sync.WaitGroup

func run(st *store.Store, start, stop int) {
	wg.Add(1)
	fmt.Println(time.Now(), "start")
	var buf [10 * 4096]byte
	i, r := 0, false
	for i = start; i < stop; i++ {
		r = st.Set(fmt.Sprintf("%d", i), buf[0:4096-500], true)
		if !r {
			break
		}
		/*if i%10000 == 0 {
			fmt.Println(i)
		}*/
	}
	fmt.Println(time.Now(), i, r)
	wg.Done()
}

func del(st *store.Store, start, stop int) {
	wg.Add(1)
	fmt.Println(time.Now(), "del start")
	i, r := 0, false
	for i = start; i < stop; i++ {
		r = st.Del(fmt.Sprintf("%d", i))
		if !r {
			break
		}
		/*if i%10000 == 0 {
			fmt.Println(i)
		}*/
	}
	fmt.Println("del", time.Now(), i, r)
	wg.Done()
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		var buf [102400]byte
		fmt.Println(string(buf[:runtime.Stack(buf[:], true)]))
		os.Exit(1)
	}()

	size := 8 * 1024 * 1024 * 1024
	st := store.New(2*size/4096, size)

	//run(st, 0*1024*1024*1024/4096, 2*1024*1024*1024/4096)
	//run(st, 4*1024*1024*1024/4096, 8*1024*1024*1024/4096)
	//run(st, 0*2*1024*1024*1024/4096, (0+1)*3*1024*1024*1024/4096)
	//return

	for {
		for i := 0; i < 4; i++ {
			go run(st, i*1536*1024*1024/4096, (i+1)*1536*1024*1024/4096)
		}
		time.Sleep(1 * time.Second)
		wg.Wait()
		continue

		for i := 0; i < 4; i++ {
			go del(st, i*1536*1024*1024/4096, (i+1)*1536*1024*1024/4096)
		}
		time.Sleep(1 * time.Second)
		wg.Wait()
	}
}
