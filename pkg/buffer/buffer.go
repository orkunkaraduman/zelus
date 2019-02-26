package buffer

import (
	"os"
	"sync"
	"time"
)

type Buffer struct {
	mu       sync.Mutex
	data     []byte
	cancelCh chan struct{}
	maxSize  int
}

var (
	MinSize = int(os.Getpagesize())
)

func New() (b *Buffer) {
	b = &Buffer{}
	return
}

func (b *Buffer) Close() {
	b.mu.Lock()
	b.data = nil
	if b.cancelCh != nil {
		close(b.cancelCh)
	}
	b.cancelCh = nil
	b.mu.Unlock()
}

func (b *Buffer) Want(size int) (buf []byte) {
	if size < 0 {
		return
	}
	b.mu.Lock()
	if b.data == nil || len(b.data) < size {
		newSize := size * 2
		if newSize < MinSize {
			newSize = MinSize
		}
		b.data = make([]byte, newSize)
		if b.cancelCh != nil {
			close(b.cancelCh)
		}
		b.cancelCh = make(chan struct{})
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
				newSize := b.maxSize * 2
				if newSize < MinSize {
					newSize = MinSize
				}
				if len(b.data) > newSize {
					b.data = make([]byte, newSize)
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
