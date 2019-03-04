package main

type nativeMemPool struct {
}

func (p *nativeMemPool) Alloc(size int) []byte {
	return make([]byte, size)
}

func (p *nativeMemPool) Free(ptr []byte) {
}
