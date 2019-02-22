package store

type MemPool interface {
	Alloc(size int) []byte
	AllocBlock(size int) []byte
	Free(ptr []byte)
	BlockSize() int
}
