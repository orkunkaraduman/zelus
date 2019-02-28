package store

import (
	"crypto/sha256"
	"unsafe"
)

func HashFunc(b []byte) (result int) {
	sum := sha256.Sum256(b)
	for i := int(unsafe.Sizeof(result)) - 1; i >= 0; i-- {
		result <<= 8
		result |= int(sum[i])
	}
	if result < 0 {
		result = -result
	}
	return
}
