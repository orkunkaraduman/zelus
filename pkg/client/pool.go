package client

import (
	"sync"
)

type Pool struct {
	mu       sync.Mutex
	max      int
	clients  map[string]map[int]*Client
	clientsI int
}

func NewPool(max int) (p *Pool) {
	p = &Pool{
		max:     max,
		clients: make(map[string]map[int]*Client, 1024),
	}
	return
}

func (p *Pool) Close() {
	for _, cls := range p.clients {
		for _, cl := range cls {
			cl.Close()
		}
	}
}

func (p *Pool) Get(network, address string) (cl *Client) {
	a := network + "://" + address
	p.mu.Lock()
	cls := p.clients[a]
	if cls == nil {
		p.mu.Unlock()
		return
	}
	for i := range cls {
		cl = cls[i]
		delete(cls, i)
		break
	}
	if len(cls) == 0 {
		delete(p.clients, a)
	}
	p.mu.Unlock()
	return
}

func (p *Pool) Put(cl *Client) {
	if cl == nil {
		return
	}
	a := cl.network + "://" + cl.address
	p.mu.Lock()
	cls := p.clients[a]
	if len(cls) >= p.max {
		cl.Close()
		p.mu.Unlock()
		return
	}
	if cls == nil {
		cls = make(map[int]*Client, p.max)
		p.clients[a] = cls
	}
	i := p.clientsI
	p.clientsI++
	cls[i] = cl
	p.mu.Unlock()
}
