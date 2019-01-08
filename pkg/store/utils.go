package store

func getBKey(key string) []byte {
	var bKey [256]byte
	keyLen := len([]byte(key))
	if keyLen <= 0 || keyLen >= len(bKey) {
		return nil
	}
	bKey[0] = byte(keyLen)
	copy(bKey[1:], []byte(key))
	return bKey[:keyLen+1]
}
