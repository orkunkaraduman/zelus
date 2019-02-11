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
	MinSize = int(os.Getpagesize())
)

func New() (b *Buffer) {
	b = &Buffer{}
	b.newData(MinSize)
	return
}

func (b *Buffer) Close() {
	b.cancelCh <- struct{}{}
	close(b.cancelCh)
	b.mu.Lock()
	b.data = nil
	b.mu.Unlock()
}

func (b *Buffer) newData(newSize int) {
	b.data = make([]byte, newSize)
	if b.cancelCh != nil {
		close(b.cancelCh)
	}
	b.cancelCh = make(chan struct{})
	go b.disposer(b.cancelCh)
}

func (b *Buffer) Want(size int) (buf []byte) {
	b.mu.Lock()
	if len(b.data) < size {
		b.newData(size * 2)
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
				if l < MinSize {
					l = MinSize
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
