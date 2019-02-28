package buffer

import (
	"sync"
)

type Pool struct {
	mu sync.RWMutex
	qu chan *Buffer
}

func NewPool(max int) (p *Pool) {
	if max <= 0 {
		return
	}
	p = &Pool{
		qu: make(chan *Buffer, max),
	}
	return
}

func (p *Pool) Close() {
	p.mu.Lock()
	if p.qu == nil {
		p.mu.Unlock()
		return
	}
	for len(p.qu) > 0 {
		bf := <-p.qu
		bf.Close()
	}
	close(p.qu)
	p.qu = nil
	p.mu.Unlock()
}

func (p *Pool) Get() (bf *Buffer) {
	p.mu.RLock()
	if p.qu == nil {
		p.mu.RUnlock()
		return
	}
	select {
	case bf = <-p.qu:
	default:
	}
	p.mu.RUnlock()
	return
}

func (p *Pool) Put(bf *Buffer) {
	if bf == nil {
		return
	}
	p.mu.RLock()
	if p.qu == nil {
		p.mu.RUnlock()
		return
	}
	select {
	case p.qu <- bf:
	default:
		bf.Close()
	}
	p.mu.RUnlock()
	return
}

func (p *Pool) GetOrNew() (bf *Buffer) {
	bf = p.Get()
	if bf == nil {
		bf = New()
	}
	return
}
