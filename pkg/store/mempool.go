package store

type MemPool interface {
	Alloc(size int) []byte
	Free(ptr []byte)
}
