package main

type memPool struct {
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
	//return (*[^uint32(0) >> 1]byte)(unsafe.Pointer(&make([]byte, size)[0]))[:size]
	return make([]byte, size, 2*size)
}

func (m *memPool) Free(ptr []byte) {
}

func (m *memPool) BlockSize() int {
	return 16
}
