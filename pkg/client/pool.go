package client

import (
	"sync"
	"time"
)

type Pool struct {
	mu             sync.RWMutex
	pingInterval   time.Duration
	qu             chan *Client
	pingerCloseCh  chan struct{}
	pingerClosedCh chan struct{}
}

func NewPool(max int, pingInterval time.Duration) (p *Pool) {
	p = &Pool{
		pingInterval:   pingInterval,
		qu:             make(chan *Client, max),
		pingerCloseCh:  make(chan struct{}, 1),
		pingerClosedCh: make(chan struct{}),
	}
	go p.pinger()
	return
}

func (p *Pool) Close() {
	select {
	case p.pingerCloseCh <- struct{}{}:
	default:
	}
	<-p.pingerClosedCh
	p.mu.Lock()
	if p.qu == nil {
		p.mu.Unlock()
		return
	}
	for len(p.qu) > 0 {
		cl := <-p.qu
		cl.Close()
	}
	close(p.qu)
	p.qu = nil
	p.mu.Unlock()
}

func (p *Pool) Get() (cl *Client) {
	p.mu.RLock()
	if p.qu == nil {
		p.mu.RUnlock()
		return
	}
	select {
	case cl = <-p.qu:
	default:
	}
	p.mu.RUnlock()
	return
}

func (p *Pool) Put(cl *Client) {
	if cl == nil || cl.IsClosed() {
		return
	}
	p.mu.RLock()
	if p.qu == nil {
		p.mu.RUnlock()
		return
	}
	select {
	case p.qu <- cl:
	default:
		cl.Close()
	}
	p.mu.RUnlock()
	return
}

func (p *Pool) GetOrNew(network, address string, connectTimeout, pingTimeout time.Duration) (cl *Client, err error) {
	cl = p.Get()
	if cl == nil {
		cl, err = New(network, address, connectTimeout, pingTimeout)
	}
	return
}

func (p *Pool) pinger() {
	tk := time.NewTicker(p.pingInterval)
	for {
		done := false
		select {
		case <-tk.C:
			cl := p.Get()
			if cl != nil {
				cl.Ping()
				if !cl.IsClosed() {
					p.Put(cl)
				}
			}
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
