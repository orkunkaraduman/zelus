package store

import (
	"reflect"
	"unsafe"

	"github.com/orkunkaraduman/zelus/pkg/malloc"
)

const (
	blockSize = malloc.BlockSize

	bKeyLenHolderLen = 1
	maxBKeyLen       = 1 << (bKeyLenHolderLen * 8)
)

const (
	MaxKeyLen = maxBKeyLen - bKeyLenHolderLen
)

var (
	zeroByte   byte
	typeOfByte = reflect.TypeOf(zeroByte)

	zeroSlot   slot
	sizeOfSlot = int(unsafe.Sizeof(zeroSlot))

	zeroNode   node
	typeOfNode = reflect.TypeOf(zeroNode)
	sizeOfNode = int(unsafe.Sizeof(zeroNode))

	zeroData   []byte
	typeOfData = reflect.TypeOf(zeroData)
	sizeOfData = int(unsafe.Sizeof(zeroData))
)

var (
	NativeAlloc bool
)

func allocBlock(p MemPool, size int) []byte {
	if size <= 0 {
		return nil
	}
	size = blockSize * ((size-1)/blockSize + 1)
	return p.Alloc(size)
}

func toSlice(src interface{}, count int, typ reflect.Type) interface{} {
	var srcVal, dstVal reflect.Value
	srcVal = reflect.ValueOf(src)
	dstVal = reflect.NewAt(reflect.ArrayOf(count, typ), unsafe.Pointer(srcVal.Pointer())).Elem()
	return dstVal.Slice(0, count).Interface()
}

func getKeyLen(bKey []byte) int {
	var keyLen int
	for i, j := 0, 0; i < bKeyLenHolderLen; i++ {
		keyLen += int(bKey[i]) << uint(j)
		j += 8
	}
	return keyLen
}

func getBKey(key string) []byte {
	//hkey := (*reflect.StringHeader)(unsafe.Pointer(&key))
	pkey := []byte(key)
	keyLen := len(pkey)
	if keyLen > MaxKeyLen {
		return nil
	}
	bKey := make([]byte, bKeyLenHolderLen+keyLen)
	for i, j := 0, keyLen; i < bKeyLenHolderLen; i++ {
		bKey[i] = byte(j)
		j >>= 8
	}
	copy(bKey[bKeyLenHolderLen:], pkey)
	return bKey
}

func lhSlotLocation(N, L, S int, h int) (offset, bucketNo, bucketOffset int) {
	expL := 1 << uint(L)
	nExpL := N * expL
	offset = h % nExpL
	expL1 := expL << 1
	nExpL1 := N * expL1
	offset1 := h % nExpL1
	if offset < S || (offset1 >= nExpL && offset1 < nExpL+S) {
		offset = offset1
	}
	m, n := 0, offset/N
	for n != 0 {
		n >>= 1
		m++
	}
	bucketNo = m
	bucketOffset = offset
	if bucketNo > 0 {
		bucketOffset -= N * (1 << uint(bucketNo-1))
	}
	return
}
