package malloc

import (
	"sync"
	"unsafe"
)

type Arena struct {
	mu       sync.Mutex
	buff     []byte
	freeList map[int]int
}

func NewArena(cap int) *Arena {
	a := &Arena{
		buff:     make([]byte, cap),
		freeList: make(map[int]int),
	}
	a.freeList[0] = cap
	return a
}

func (a *Arena) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	offset, length := -1, int(^uint(0)>>1)
	for k, v := range a.freeList {
		if v >= size && v < length && k > offset {
			offset = k
			length = v
		}
	}
	if offset < 0 {
		return nil
	}
	foundOffset := offset
	ending := offset + length
	lengthHigh := highbit(length - 1)
	sizeHigh := highbit(size - 1)
	for offset < ending {
		if lengthHigh > sizeHigh {
			lengthHigh--
			length = 1 << uint(lengthHigh)
		}
		l := ending - offset
		if l < length {
			length = l
		}
		a.freeList[offset] = length
		//fmt.Println("write:", offset, length)
		if length >= size {
			foundOffset = offset
		}
		offset += length
	}
	offset = foundOffset
	length = a.freeList[offset]
	delete(a.freeList, offset)
	//fmt.Println("found:", offset, length)
	return a.buff[offset : offset+size]
}

func (a *Arena) Free(ptr []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	ptrSize := len(ptr)
	if ptrSize <= 0 {
		return
	}
	ptrOffset := int(uintptr(unsafe.Pointer(&ptr[0])) - uintptr(unsafe.Pointer(&a.buff[0])))
	if ptrOffset >= len(a.buff) {
		return
	}
	ptrHigh := highbit(ptrSize - 1)
	ptrLength := 1 << uint(ptrHigh)
	offset := ptrOffset
	length := ptrLength

	a.freeList[offset] = length
	//fmt.Println("lwrite:", offset, length)

	b := true
	for b {
		b = false
		if (offset/length)%2 == 0 {
			n := offset + length
			if l, ok := a.freeList[n]; ok && l == length {
				delete(a.freeList, n)
				length *= 2
				a.freeList[offset] = length
				//fmt.Println("fwrite:", offset, length)
				b = true
			}
		} else {
			n := offset - length
			if l, ok := a.freeList[n]; ok && l == length {
				delete(a.freeList, offset)
				offset = n
				length *= 2
				a.freeList[offset] = length
				//fmt.Println("rwrite:", offset, length)
				b = true
			}
		}
	}
}
