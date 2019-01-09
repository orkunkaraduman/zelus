package store

import (
	"sync"

	"github.com/orkunkaraduman/zelus/pkg/malloc"
)

type Store struct {
	mu       sync.RWMutex
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

func (st *Store) Get(key string) (val []byte) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	defer sl.Mu.Unlock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		return
	}
	nd := &sl.Nodes[ndIdx]
	for i, j := 0, len(nd.Datas); i < j; i++ {
		val = append(val, nd.Datas[i]...)
	}
	val = val[len(bKey):]
	return
}

func (st *Store) Set(key string, val []byte, replace bool) bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	defer sl.Mu.Unlock()
	ndIdx := sl.FindNode(keyHash, bKey)
	oldNdIdx := -1
	if ndIdx >= 0 {
		if !replace {
			return false
		}
		oldNdIdx = ndIdx
		ndIdx = sl.NewNode(st.slotPool)
	} else {
		ndIdx = sl.NewNode(st.slotPool)
	}
	if ndIdx < 0 {
		return false
	}
	nd := &sl.Nodes[ndIdx]
	nd.KeyHash = keyHash
	dataIdx := nd.Alloc(st.slotPool, st.dataPool, len(bKey)+len(val))
	if dataIdx < 0 {
		sl.DelNode(st.slotPool, st.dataPool, ndIdx)
		return false
	}
	for bKeyIdx, valIdx := 0, 0; dataIdx < len(nd.Datas); dataIdx++ {
		data := nd.Datas[dataIdx]
		n := 0
		if bKeyIdx < len(bKey) {
			n = copy(data, bKey)
			bKeyIdx += n
		}
		if n < len(data) {
			valIdx += copy(data[n:], val[valIdx:])
		}
	}
	if oldNdIdx >= 0 {
		sl.DelNode(st.slotPool, st.dataPool, oldNdIdx)
	}
	return true
}

func (st *Store) Append(key string, val []byte) bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	defer sl.Mu.Unlock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		return false
	}
	nd := &sl.Nodes[ndIdx]
	dataIdx := nd.Alloc(st.slotPool, st.dataPool, len(val))
	if dataIdx < 0 {
		return false
	}
	for valIdx := 0; dataIdx < len(nd.Datas); dataIdx++ {
		data := nd.Datas[dataIdx]
		valIdx += copy(data, val[valIdx:])
	}
	return true
}

func (st *Store) Del(key string) bool {
	st.mu.RLock()
	defer st.mu.RUnlock()
	bKey := getBKey(key)
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	defer sl.Mu.Unlock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		return false
	}
	sl.DelNode(st.slotPool, st.dataPool, ndIdx)
	return true
}
