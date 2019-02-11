package store

func getBKey(key string) []byte {
	var bKey [256]byte
	keyLen := len([]byte(key))
	if keyLen >= len(bKey) {
		return nil
	}
	bKey[0] = byte(keyLen)
	copy(bKey[1:], []byte(key))
	return bKey[:keyLen+1]
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
