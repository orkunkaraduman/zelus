package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
)

type queue struct {
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

func newQueue(addr string, connectTimeout, pingTimeout time.Duration, connectRetryCount int, maxLen, maxSize int,
	cmdName string, standalone bool) (q *queue) {
	q = &queue{
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
	if q.maxLen < 0 {
		q.maxLen = 0
	}
	if q.maxSize < 0 {
		q.maxSize = 0
	}
	q.qu = make(chan keyVal, q.maxLen)
	go q.pinger()
	go q.worker()
	return
}

func (q *queue) Close() {
	select {
	case q.pingerCloseCh <- struct{}{}:
	default:
	}
	<-q.pingerClosedCh
	select {
	case q.workerCloseCh <- struct{}{}:
	default:
	}
	<-q.workerClosedCh
	if q.cl != nil {
		q.cl.Close()
	}
}

func (q *queue) checkClient() (err error) {
	q.mu.Lock()
	if q.cl != nil && !q.cl.IsClosed() {
		q.mu.Unlock()
		return
	}
	err = errors.New("client closed")
	for i := 0; err != nil && i < q.connectRetryCount; i++ {
		q.cl, err = client.New("tcp", q.addr, q.connectTimeout, q.pingTimeout)
	}
	if err == nil && q.standalone {
		err = q.cl.Standalone()
	}
	if err != nil {
		q.cl = nil
	}
	q.mu.Unlock()
	return
}

func (q *queue) pinger() {
	tk := time.NewTicker(15 * time.Second)
	for {
		done := false
		select {
		case <-tk.C:
			q.mu.RLock()
			if q.cl != nil {
				q.cl.Ping()
			}
			q.mu.RUnlock()
		case <-q.pingerCloseCh:
			done = true
		}
		if done {
			break
		}
	}
	tk.Stop()
	close(q.pingerClosedCh)
}

func (q *queue) worker() {
	maxKeyCount := 128
	keys := make([]string, 0, maxKeyCount)
	kvs := make([]keyVal, 0, maxKeyCount)
	for {
		done := false
		select {
		case kv := <-q.qu:
			l := len(q.qu) + 1
			if l > maxKeyCount {
				l = maxKeyCount
			}
			keys = keys[:l]
			kvs = kvs[:l]
			keys[0] = kv.Key
			kvs[0] = kv
			for i := 1; i < l; i++ {
				kv := <-q.qu
				keys[i] = kv.Key
				kvs[i] = kv
			}
			e := q.checkClient()
			if e == nil {
				var k []string
				switch q.cmdName {
				case "GET":
					k = make([]string, 0, len(keys))
					e = q.cl.Get(keys, func(index int, key string, val []byte, expires int) {
						if val != nil {
							k = append(k, key)
							kvs[index].Val = make([]byte, len(val))
							copy(kvs[index].Val, val)
						} else {
							kvs[index].Val = nil
						}
						kvs[index].Expires = expires
					})
				case "SET":
					k, e = q.cl.Set(keys, func(index int, key string) (val []byte, expires int) {
						return kvs[index].Val, kvs[index].Expires
					})
				case "PUT":
					k, e = q.cl.Put(keys, func(index int, key string) (val []byte, expires int) {
						return kvs[index].Val, kvs[index].Expires
					})
				case "APPEND":
					k, e = q.cl.Append(keys, func(index int, key string) (val []byte, expires int) {
						return kvs[index].Val, kvs[index].Expires
					})
				default:
					e = errors.New("unexpected command")
				}
				if e == nil {
					i, j := 0, len(k)
					for idx := range kvs {
						if i >= j || k[i] != kvs[idx].Key {
							q.remove(kvs[idx], nil)
							continue
						}
						q.remove(kvs[idx], kvs[idx])
						kvs[idx].Val = nil
						i++
					}
				}
			}
			if e != nil {
				for _, kv := range kvs {
					q.remove(kv, e)
				}
			}
		case <-q.workerCloseCh:
			done = true
		}
		if done {
			break
		}
	}
	close(q.workerClosedCh)
}

func (sq *queue) remove(kv keyVal, cb interface{}) {
	atomic.AddInt64(&sq.quSize, int64(-len(kv.Val)))
	if kv.CallBack != nil {
		kv.CallBack <- cb
	}
}

func (sq *queue) Add(kv keyVal) {
	for sq.maxSize > 0 && sq.quSize > int64(sq.maxSize) {
		time.Sleep(5 * time.Millisecond)
	}
	atomic.AddInt64(&sq.quSize, int64(len(kv.Val)))
	sq.qu <- kv
}
