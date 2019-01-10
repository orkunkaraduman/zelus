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
	l := 0
	for _, data := range nd.Datas {
		l += len(data)
	}
	p := len(bKey)
	l -= p
	m := 0
	val = make([]byte, l)
	for _, data := range nd.Datas {
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
	datasIdx := nd.Alloc(st.slotPool, st.dataPool, len(bKey)+len(val))
	if datasIdx < 0 {
		sl.DelNode(st.slotPool, st.dataPool, ndIdx)
		return false
	}
	j, m := 0, 0
	for _, data := range nd.Datas {
		n := 0
		if j < len(bKey) {
			n = copy(data, bKey)
			j += n
		}
		if n < len(data) {
			m += copy(data[n:], val[m:])
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
	datasIdx := nd.Alloc(st.slotPool, st.dataPool, len(val))
	if datasIdx < 0 {
		return false
	}
	valIdx := 0
	for _, data := range nd.Datas {
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
