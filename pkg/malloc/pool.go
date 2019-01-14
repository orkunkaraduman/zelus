package malloc

import (
	"os"
	"sync"
	"sync/atomic"
	"unsafe"
)

type Pool struct {
	mu     sync.RWMutex
	arenas []*Arena
	stats  PoolStats
}

type PoolStats struct {
	TotalSize     int64
	AllocatedSize int64
	RequestedSize int64
}

func NewPool() *Pool {
	p := &Pool{}
	return p
}

func AllocPool(totalSize int) *Pool {
	p := NewPool()
	p.Grow(totalSize)
	return p
}

func (p *Pool) Grow(n int) int {
	if n <= 0 {
		panic(ErrSizeMustBePositive)
	}
	pagesize := os.Getpagesize()
	n = ((n-1)/minLength + 1) * minLength
	buf := make([]byte, n)
	for i, j := 0, len(buf); i < j; i += pagesize {
		buf[i] = 0
	}
	p.mu.Lock()
	m := 0
	for n > 0 {
		length := 1 << uint(HighBit(n)-1)
		if length < minLength {
			length = minLength
		}
		p.arenas = append(p.arenas, NewArena(buf[m:m+length]))
		m += length
		n -= length
	}
	p.mu.Unlock()
	atomic.AddInt64(&p.stats.TotalSize, int64(m))
	return m
}

func (p *Pool) alloc(size int, block bool) []byte {
	p.mu.RLock()
	var ptr []byte
	for _, a := range p.arenas {
		ss := a.Stats()
		if ss.TotalSize-ss.AllocatedSize < int64(size) {
			continue
		}
		ptr = a.alloc(size, block)
		if ptr != nil {
			atomic.AddInt64(&p.stats.AllocatedSize, 1<<uint(HighBit(len(ptr)-1)))
			atomic.AddInt64(&p.stats.RequestedSize, int64(len(ptr)))
			break
		}
	}
	p.mu.RUnlock()
	return ptr
}

func (p *Pool) Alloc(size int) []byte {
	return p.alloc(size, false)
}

func (p *Pool) AllocBlock(size int) []byte {
	return p.alloc(size, true)
}

func (p *Pool) Free(ptr []byte) {
	if ptr == nil {
		return
	}
	p.mu.RLock()
	for _, a := range p.arenas {
		if uintptr(unsafe.Pointer(&ptr[0])) >= uintptr(unsafe.Pointer(&a.buf[0])) &&
			uintptr(unsafe.Pointer(&ptr[0])) < uintptr(unsafe.Pointer(&a.buf[0]))+uintptr(len(a.buf)) {
			a.Free(ptr)
			atomic.AddInt64(&p.stats.AllocatedSize, -int64(1<<uint(HighBit(len(ptr)-1))))
			atomic.AddInt64(&p.stats.RequestedSize, -int64(len(ptr)))
		}
	}
	p.mu.RUnlock()
}

func (p *Pool) Stats() (stats PoolStats) {
	stats = p.stats
	return
}
