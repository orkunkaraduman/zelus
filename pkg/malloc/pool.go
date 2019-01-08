package malloc

import (
	"sync"
	"unsafe"
)

type Pool struct {
	mu     sync.Mutex
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

func (p *Pool) Grow(n int) {
	if n <= 0 {
		panic(ErrSizeMustBePositive)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	buf := make([]byte, n)
	offset := 0
	for offset < n {
		length := 1 << uint(HighBit(n-offset)-1)
		p.arenas = append(p.arenas, NewArena(buf[offset:offset+length]))
		offset += length
	}
	p.stats.TotalSize += n
}

func (p *Pool) alloc(size int, block bool) []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	var ptr []byte
	for _, a := range p.arenas {
		ptr = a.alloc(size, block)
		if ptr != nil {
			p.stats.AllocatedSize += 1 << uint(HighBit(len(ptr)-1))
			p.stats.RequestedSize += len(ptr)
			break
		}
	}
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
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, a := range p.arenas {
		if uintptr(unsafe.Pointer(&ptr[0])) >= uintptr(unsafe.Pointer(&a.buf[0])) &&
			uintptr(unsafe.Pointer(&ptr[0])) < uintptr(unsafe.Pointer(&a.buf[0]))+uintptr(len(a.buf)) {
			a.Free(ptr)
			p.stats.AllocatedSize -= 1 << uint(HighBit(len(ptr)-1))
			p.stats.RequestedSize -= len(ptr)
		}
	}
}

func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}
