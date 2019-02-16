package server

/*import (
	"sync"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
)

type slaveHandler struct {
	C chan keyVal

	mu      sync.RWMutex
	workers map[string]*slaveWorker
	closeCh chan struct{}
}

const (
	slaveHandlerQueueLen = 16 * 1024
)

func newSlaveHandler() (sh *slaveHandler) {
	sh = &slaveHandler{
		C:       make(chan keyVal, slaveHandlerQueueLen),
		workers: make(map[string]*slaveWorker, 1024),
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
			for addr, sw := range sh.workers {
				select {
				case <-sh.closeCh:
					sh.mu.RUnlock()
					return
				case sw.C <- kv:
				case <-time.After(250 * time.Millisecond):
					go sh.Remove(addr)
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
	for addr, sw := range sh.workers {
		delete(sh.workers, addr)
		sw.Close()
	}
	sh.mu.Unlock()
}

func (sh *slaveHandler) Add(addr string) (cl *client.Client) {
	sh.mu.Lock()
	if _, ok := sh.workers[addr]; ok {
		sh.mu.Unlock()
		return
	}
	cl, _ = client.New("tcp", addr, 1*time.Second)
	if cl == nil {
		sh.mu.Unlock()
		return
	}
	sh.workers[addr] = newSlaveWorker(addr, cl)
	sh.mu.Unlock()
	return
}

func (sh *slaveHandler) Remove(addr string) bool {
	sh.mu.Lock()
	var sw *slaveWorker
	var ok bool
	if sw, ok = sh.workers[addr]; !ok {
		sh.mu.Unlock()
		return false
	}
	delete(sh.workers, addr)
	sw.Close()
	sh.mu.Unlock()
	return true
}*/
