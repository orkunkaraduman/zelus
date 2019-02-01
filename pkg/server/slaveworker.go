package server

import (
	"context"
	"sync"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
)

type slaveWorker struct {
	C chan keyVal

	mu      sync.Mutex
	addr    string
	cl      *client.Client
	closeCh chan struct{}
}

func newSlaveWorker(addr string, cl *client.Client) (sw *slaveWorker) {
	sw = &slaveWorker{
		C:       make(chan keyVal, 2*slaveHandlerQueueLen),
		addr:    addr,
		cl:      cl,
		closeCh: make(chan struct{}, 1),
	}
	go sw.run()
	return
}

func (sw *slaveWorker) Close() {
	sw.mu.Lock()
	select {
	case sw.closeCh <- struct{}{}:
	default:
	}
	if sw.cl != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		sw.cl.Shutdown(ctx)
	}
	sw.mu.Unlock()
}

func (sw *slaveWorker) run() {
	multi := 128
	keys := make([]string, 0, multi)
	kvs := make([]keyVal, 0, multi)
	for {
		select {
		case <-sw.closeCh:
			return
		case kv := <-sw.C:
			keys = keys[:0]
			for len(sw.closeCh) == 0 {
				if sw.cl == nil || sw.cl.IsClosed() {
					cl, _ := client.New("tcp", sw.addr)
					if cl == nil {
						time.Sleep(250 * time.Millisecond)
						continue
					}
					sw.mu.Lock()
					if len(sw.closeCh) != 0 {
						cl.Close()
						sw.mu.Unlock()
						break
					}
					sw.cl = cl
					sw.mu.Unlock()
				}
				if len(keys) == 0 {
					l := len(sw.C) + 1
					if l > multi {
						l = multi
					}
					keys = keys[:l]
					kvs = kvs[:l]
					keys[0] = kv.Key
					kvs[0] = kv
					for i := 1; i < l; i++ {
						kv := <-sw.C
						keys[i] = kv.Key
						kvs[i] = kv
					}
				}
				k, _ := sw.cl.Set(keys, func(index int, key string) (val []byte, expiry int) {
					kv := kvs[index]
					return kv.Val, kv.Expiry
				})
				if len(k) != len(keys) {
					time.Sleep(250 * time.Millisecond)
					continue
				}
				break
			}
		}
	}
}
