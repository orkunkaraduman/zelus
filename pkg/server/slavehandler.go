package server

import (
	"fmt"
	"sync"
	"time"
)

type slaveHandler struct {
	C chan keyVal

	mu          sync.RWMutex
	slaves      map[string][]*slaveWorker
	clientCount int
	closeCh     chan struct{}
}

const (
	slaveHandlerQueueLen = 1 * 1024
)

func newSlaveHandler() (sh *slaveHandler) {
	sh = &slaveHandler{
		C:       make(chan keyVal, slaveHandlerQueueLen),
		slaves:  make(map[string][]*slaveWorker, 1024),
		closeCh: make(chan struct{}, 1),
	}
	go sh.sender()
	return
}

func (sh *slaveHandler) sender() {
	for {
		select {
		case <-sh.closeCh:
			return
		case kv := <-sh.C:
			sh.mu.RLock()
			for addr, sv := range sh.slaves {
				for _, sw := range sv {
					select {
					case <-sh.closeCh:
						sh.mu.RUnlock()
						return
					case sw.C <- kv:
					case <-time.After(250 * time.Millisecond):
						fmt.Println(len(sw.C))
						go sh.Remove(addr)
					}
				}
			}
			sh.mu.RUnlock()
		}
	}
}

func (sh *slaveHandler) Close() {
	select {
	case sh.closeCh <- struct{}{}:
	default:
	}
	sh.mu.Lock()
	for addr, sv := range sh.slaves {
		delete(sh.slaves, addr)
		for i := range sv {
			sv[i].Close()
		}
	}
	sh.mu.Unlock()
}

func (sh *slaveHandler) Add(addr string) bool {
	sh.mu.Lock()
	if _, ok := sh.slaves[addr]; ok {
		sh.mu.Unlock()
		return false
	}
	sv := make([]*slaveWorker, sh.clientCount)
	sh.slaves[addr] = sv
	for i := range sv {
		sv[i] = newSlaveWorker(addr)
	}
	sh.mu.Unlock()
	return true
}

func (sh *slaveHandler) Remove(addr string) bool {
	sh.mu.Lock()
	if _, ok := sh.slaves[addr]; !ok {
		sh.mu.Unlock()
		return false
	}
	sv := sh.slaves[addr]
	delete(sh.slaves, addr)
	for _, sw := range sv {
		sw.Close()
	}
	sh.mu.Unlock()
	return true
}

func (sh *slaveHandler) Inc() {
	sh.mu.Lock()
	for addr, sv := range sh.slaves {
		sh.slaves[addr] = append(sv, newSlaveWorker(addr))
	}
	sh.clientCount++
	sh.mu.Unlock()
}

func (sh *slaveHandler) Dec() {
	sh.mu.Lock()
	for addr, sv := range sh.slaves {
		l := len(sv)
		if l <= 0 {
			continue
		}
		l--
		sv[l].Close()
		sh.slaves[addr] = sv[:l]
	}
	sh.clientCount--
	sh.mu.Unlock()
}
