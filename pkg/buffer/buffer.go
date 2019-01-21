package buffer

import (
	"os"
	"sync"
	"time"
)

type Buffer struct {
	mu       sync.Mutex
	data     []byte
	maxSize  int
	cancelCh chan struct{}
}

var (
	minSize = os.Getpagesize()
)

func New() (b *Buffer) {
	b = &Buffer{
		data:     make([]byte, minSize),
		cancelCh: make(chan struct{}, 1),
	}
	return
}

func (b *Buffer) Close() {
	b.mu.Lock()
	b.data = nil
	close(b.cancelCh)
	b.mu.Unlock()
}

func (b *Buffer) Need(size int) (buf []byte) {
	b.mu.Lock()
	if len(b.data) < size {
		b.data = make([]byte, size*2)
		close(b.cancelCh)
		b.cancelCh = make(chan struct{}, 1)
		go b.disposer(b.cancelCh)
	}
	buf = b.data[:size]
	if size > b.maxSize {
		b.maxSize = size
	}
	b.mu.Unlock()
	return
}

func (b *Buffer) disposer(c chan struct{}) {
	tk := time.NewTicker(60 * time.Second)
	for {
		done := false
		select {
		case <-tk.C:
			b.mu.Lock()
			if len(b.data)/4 >= b.maxSize {
				l := b.maxSize * 2
				if l < minSize {
					l = minSize
				}
				if len(b.data) > l {
					b.data = make([]byte, l)
				}
			}
			b.maxSize = 0
			b.mu.Unlock()
		case <-c:
			done = true
		}
		if done {
			break
		}
	}
	tk.Stop()
}