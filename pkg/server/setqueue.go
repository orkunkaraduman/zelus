package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
)

type setQueue struct {
	mu                          sync.RWMutex
	addr                        string
	connectTimeout, pingTimeout time.Duration
	connectRetryCount           int
	maxLen, maxSize             int
	cmdName                     string
	standalone                  bool
	cl                          *client.Client
	qu                          chan keyVal
	quSize                      int64
	pingerCloseCh               chan struct{}
	pingerClosedCh              chan struct{}
	workerCloseCh               chan struct{}
	workerClosedCh              chan struct{}
}

var errSetQueueRemoteSet = errors.New("remote SET error")

func NewSetQueue(addr string, connectTimeout, pingTimeout time.Duration, connectRetryCount int, maxLen, maxSize int,
	cmdName string, standalone bool) (sq *setQueue) {
	sq = &setQueue{
		addr:              addr,
		connectTimeout:    connectTimeout,
		pingTimeout:       pingTimeout,
		connectRetryCount: connectRetryCount,
		maxLen:            maxLen,
		maxSize:           maxSize,
		cmdName:           cmdName,
		standalone:        standalone,
		pingerCloseCh:     make(chan struct{}, 1),
		pingerClosedCh:    make(chan struct{}),
		workerCloseCh:     make(chan struct{}, 1),
		workerClosedCh:    make(chan struct{}),
	}
	if sq.maxLen < 0 {
		sq.maxLen = 0
	}
	if sq.maxSize < 0 {
		sq.maxSize = 0
	}
	sq.qu = make(chan keyVal, sq.maxLen)
	go sq.pinger()
	go sq.worker()
	return
}

func (sq *setQueue) Close() {
	select {
	case sq.pingerCloseCh <- struct{}{}:
	default:
	}
	<-sq.pingerClosedCh
	select {
	case sq.workerCloseCh <- struct{}{}:
	default:
	}
	<-sq.workerClosedCh
	if sq.cl != nil {
		sq.cl.Close()
	}
}

func (sq *setQueue) checkClient() (err error) {
	if sq.cl != nil && !sq.cl.IsClosed() {
		return
	}
	sq.cl = nil
	for i := 0; sq.cl == nil && i < sq.connectRetryCount; i++ {
		sq.cl, err = client.New("tcp", sq.addr, sq.connectTimeout, sq.pingTimeout)
	}
	if err == nil && sq.standalone {
		err = sq.cl.Standalone()
	}
	if err != nil {
		sq.cl = nil
	}
	return
}

func (sq *setQueue) pinger() {
	tk := time.NewTicker(15 * time.Second)
	for {
		done := false
		select {
		case <-tk.C:
			sq.mu.RLock()
			if sq.cl != nil {
				sq.cl.Ping()
			}
			sq.mu.RUnlock()
		case <-sq.pingerCloseCh:
			done = true
		}
		if done {
			break
		}
	}
	tk.Stop()
	close(sq.pingerClosedCh)
}

func (sq *setQueue) worker() {
	multi := 128
	keys := make([]string, 0, multi)
	kvs := make([]keyVal, 0, multi)
	for {
		done := false
		select {
		case kv := <-sq.qu:
			l := len(sq.qu) + 1
			if l > multi {
				l = multi
			}
			keys = keys[:l]
			kvs = kvs[:l]
			keys[0] = kv.Key
			kvs[0] = kv
			for i := 1; i < l; i++ {
				kv := <-sq.qu
				keys[i] = kv.Key
				kvs[i] = kv
			}
			sq.mu.Lock()
			e := sq.checkClient()
			sq.mu.Unlock()
			if e == nil {
				var k []string
				switch sq.cmdName {
				case "SET":
					k, e = sq.cl.Set(keys, func(index int, key string) (val []byte, expires2 int) {
						return kvs[index].Val, kvs[index].Expires
					})
				case "PUT":
					k, e = sq.cl.Put(keys, func(index int, key string) (val []byte, expires2 int) {
						return kvs[index].Val, kvs[index].Expires
					})
				case "APPEND":
					k, e = sq.cl.Append(keys, func(index int, key string) (val []byte, expires2 int) {
						return kvs[index].Val, kvs[index].Expires
					})
				default:
					e = errors.New("unexpected command")
				}
				if e == nil {
					i, j := 0, len(k)
					for _, kv := range kvs {
						if i >= j || k[i] != kv.Key {
							sq.remove(kv, errSetQueueRemoteSet)
							continue
						}
						sq.remove(kv, nil)
						i++
					}
				}
			}
			if e != nil {
				for _, kv := range kvs {
					sq.remove(kv, e)
				}
			}
		case <-sq.workerCloseCh:
			done = true
		}
		if done {
			break
		}
	}
	close(sq.workerClosedCh)
}

func (sq *setQueue) remove(kv keyVal, e error) {
	atomic.AddInt64(&sq.quSize, int64(-len(kv.Val)))
	if kv.CallBack != nil {
		kv.CallBack <- e
	}
}

func (sq *setQueue) Add(kv keyVal) {
	for sq.maxSize > 0 && sq.quSize > int64(sq.maxSize) {
		time.Sleep(5 * time.Millisecond)
	}
	atomic.AddInt64(&sq.quSize, int64(len(kv.Val)))
	sq.qu <- kv
}
