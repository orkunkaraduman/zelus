package malloc

import (
	"sync"
	"unsafe"
)

type Pool struct {
	mu     sync.RWMutex
	arenas []*Arena
	stats  PoolStats
}

type PoolStats struct {
	TotalSize     int
	AllocatedSize int
	RequestedSize int
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
	k := n / minLength
	if n%minLength != 0 {
		k++
	}
	n = k * minLength
	buf := make([]byte, n)
	for i, j := 0, len(buf); i < j; i += 1024 {
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
	p.stats.TotalSize += m
	p.mu.Unlock()
	return m
}

func (p *Pool) alloc(size int, block bool) []byte {
	p.mu.RLock()
	var ptr []byte
	for _, a := range p.arenas {
		ptr = a.alloc(size, block)
		if ptr != nil {
			p.stats.AllocatedSize += 1 << uint(HighBit(len(ptr)-1))
			p.stats.RequestedSize += len(ptr)
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
			p.stats.AllocatedSize -= 1 << uint(HighBit(len(ptr)-1))
			p.stats.RequestedSize -= len(ptr)
		}
	}
	p.mu.RUnlock()
}

func (p *Pool) Stats() (stats PoolStats) {
	p.mu.Lock()
	stats = p.stats
	p.mu.Unlock()
	return
}
