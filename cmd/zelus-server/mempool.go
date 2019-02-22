package main

type memPool struct {
	p *byte
}

func (m *memPool) Alloc(size int) []byte {
	return m.AllocBlock(size)[:size]
}

func (m *memPool) AllocBlock(size int) []byte {
	if size < 0 {
		return nil
	}
	bs := m.BlockSize()
	size = bs * (size/bs + 1)
	return make([]byte, size)
}

func (m *memPool) Free(ptr []byte) {
}

func (m *memPool) BlockSize() int {
	return 16
}
