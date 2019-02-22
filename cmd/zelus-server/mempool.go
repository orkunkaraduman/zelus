package main

type memPool struct {
}

func (m *memPool) Alloc(size int) []byte {
	return make([]byte, size)
}

func (m *memPool) Free(ptr []byte) {
}
