package store

import (
	"crypto/sha256"
	"unsafe"
)

func HashFunc(b []byte) (result int) {
	h := sha256.New()
	h.Write(b)
	sum := h.Sum(nil)
	for i := int(unsafe.Sizeof(result)) - 1; i >= 0; i-- {
		result <<= 8
		result |= int(sum[i])
	}
	if result < 0 {
		result = -result
	}
	return
}
