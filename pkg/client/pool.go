package client

import (
	"sync"
	"time"
)

type Pool struct {
	mu             sync.Mutex
	max            int
	clients        map[string]map[int]*Client
	clientsI       int
	pingerCloseCh  chan struct{}
	pingerClosedCh chan struct{}
}

func NewPool(max int) (p *Pool) {
	p = &Pool{
		max:            max,
		clients:        make(map[string]map[int]*Client, 1024),
		pingerCloseCh:  make(chan struct{}, 1),
		pingerClosedCh: make(chan struct{}),
	}
	go p.pinger()
	return
}

func (p *Pool) pinger() {
	tk := time.NewTicker(60 * time.Second)
	for {
		done := false
		select {
		case <-tk.C:
			p.mu.Lock()
			i := 0
			c := make(map[int]*Client, 1024*p.max)
			for _, cls := range p.clients {
				for _, cl := range cls {
					c[i] = cl
					i++
				}
			}
			p.mu.Unlock()
			for _, cl := range c {
				if len(p.pingerCloseCh) != 0 {
					break
				}
				cl.Ping(nil)
			}
			if len(p.pingerCloseCh) != 0 {
				break
			}
			p.mu.Lock()
			for a, cls := range p.clients {
				for i, cl := range cls {
					if cl.IsClosed() {
						delete(cls, i)
					}
				}
				if len(cls) == 0 {
					delete(p.clients, a)
				}
			}
			p.mu.Unlock()
		case <-p.pingerCloseCh:
			done = true
		}
		if done {
			break
		}
	}
	tk.Stop()
	close(p.pingerClosedCh)
}

func (p *Pool) Close() {
	select {
	case p.pingerCloseCh <- struct{}{}:
	default:
	}
	<-p.pingerClosedCh
	p.mu.Lock()
	for _, cls := range p.clients {
		for _, cl := range cls {
			cl.Close()
		}
	}
	p.mu.Unlock()
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
		if cl.IsClosed() {
			cl = nil
		} else {
			break
		}
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
