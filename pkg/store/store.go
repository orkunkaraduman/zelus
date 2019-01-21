package store

import (
	"github.com/orkunkaraduman/zelus/pkg/malloc"
)

type Store struct {
	slots    []slot
	slotPool *malloc.Pool
	dataPool *malloc.Pool
}

func New(count int, size int) (st *Store) {
	if count <= 0 {
		return
	}
	p := malloc.AllocPool(size)
	st = &Store{
		slots:    make([]slot, count),
		slotPool: p,
		dataPool: p,
	}
	return
}

func (st *Store) Get(key string, buf []byte) (val []byte) {
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		sl.Mu.Unlock()
		return
	}
	nd := &sl.Nodes[ndIdx]
	l := nd.Size
	p := len(bKey)
	l -= p
	if len(buf) >= l {
		val = buf[:l]
	} else {
		val = make([]byte, l)
	}
	m := 0
	for _, data := range nd.Datas {
		if data == nil {
			break
		}
		n, r := 0, len(data)
		if p != 0 {
			if r > p {
				r = p
			}
			n += r
			p -= r
		}
		if p == 0 {
			copy(val[m:], data[n:])
		}
	}
	sl.Mu.Unlock()
	return
}

func (st *Store) Set(key string, val []byte, replace bool) bool {
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	ndIdx := -1
	foundNdIdx := sl.FindNode(keyHash, bKey)
	if foundNdIdx >= 0 {
		if !replace {
			sl.Mu.Unlock()
			return false
		}
		ndIdx = foundNdIdx
	} else {
		ndIdx = sl.NewNode(st.slotPool)
	}
	if ndIdx < 0 {
		sl.Mu.Unlock()
		return false
	}
	nd := &sl.Nodes[ndIdx]
	nd.KeyHash = keyHash
	if !nd.Set(st.slotPool, st.dataPool, len(bKey)+len(val)) {
		if foundNdIdx < 0 {
			sl.DelNode(st.slotPool, st.dataPool, ndIdx)
		}
		sl.Mu.Unlock()
		return false
	}
	j, m := 0, 0
	for _, data := range nd.Datas {
		if data == nil {
			break
		}
		n := 0
		if j < len(bKey) {
			n = copy(data, bKey[j:])
			j += n
		}
		if n < len(data) {
			m += copy(data[n:], val[m:])
		}
	}
	sl.Mu.Unlock()
	return true
}

func (st *Store) Del(key string) bool {
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		sl.Mu.Unlock()
		return false
	}
	sl.DelNode(st.slotPool, st.dataPool, ndIdx)
	sl.Mu.Unlock()
	return true
}

func (st *Store) Append(key string, val []byte) bool {
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		sl.Mu.Unlock()
		return false
	}
	nd := &sl.Nodes[ndIdx]
	index, offset := nd.Last()
	if !nd.Alloc(st.slotPool, st.dataPool, len(val)) {
		sl.Mu.Unlock()
		return false
	}
	valIdx, valLen := 0, len(val)
	for valIdx < valLen {
		valIdx += copy(nd.Datas[index][offset:], val[valIdx:])
		index++
		offset = 0
	}
	sl.Mu.Unlock()
	return true
}
