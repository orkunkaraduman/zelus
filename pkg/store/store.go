package store

import (
	"sync/atomic"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/malloc"
)

type Store struct {
	slots      []slot
	slotPool   *malloc.Pool
	dataPool   *malloc.Pool
	disposerCh chan struct{}
	done       int32
}

type GetFunc func(size int, index int, data []byte, expiry int)

type updateAction int

const (
	updateActionNone = updateAction(iota)
	updateActionReplace
	updateActionAppend
)

func New(count int, size int) (st *Store) {
	if count <= 0 {
		return
	}
	p := malloc.AllocPool(size)
	st = &Store{
		slots:      make([]slot, count),
		slotPool:   p,
		dataPool:   p,
		disposerCh: make(chan struct{}),
	}
	go st.disposer()
	return
}

func (st *Store) Close() {
	atomic.StoreInt32(&st.done, 1)
	select {
	case st.disposerCh <- struct{}{}:
	default:
	}
	st.slotPool.Close()
	st.dataPool.Close()
}

func (st *Store) disposer() {
	tk := time.NewTicker(60 * time.Second)
	for st.done == 0 {
		select {
		case <-tk.C:
		case <-st.disposerCh:
		}
		for i := range st.slots {
			if st.done != 0 {
				break
			}
			sl := &st.slots[i]
			sl.Mu.Lock()
			for j := 0; j < len(sl.Nodes); j++ {
				if st.done != 0 {
					break
				}
				nd := &sl.Nodes[j]
				if nd.KeyHash >= 0 && nd.Expiry >= 0 && nd.Expiry < int(time.Now().Unix()) {
					sl.DelNode(st.slotPool, st.dataPool, j)
				}
			}
			sl.Mu.Unlock()
		}
	}
	tk.Stop()
}

func (st *Store) Get(key string, f GetFunc) bool {
	bKey := getBKey(key)
	if bKey == nil {
		return false
	}
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
	if nd.Expiry >= 0 && nd.Expiry < int(time.Now().Unix()) {
		sl.Mu.Unlock()
		return false
	}
	p := len(bKey)
	valIdx, valLen := 0, nd.Size-p
	for index := 0; valIdx < valLen; index++ {
		data := nd.Datas[index]
		n, r := 0, len(data)
		if p != 0 {
			if r > p {
				r = p
			}
			n += r
			p -= r
		}
		if p == 0 {
			d := data[n:]
			f(valLen, valIdx, d, nd.Expiry)
			valIdx += len(d)
		}
	}
	sl.Mu.Unlock()
	return true
}

func (st *Store) write(key string, val []byte, ua updateAction, expiry int) bool {
	bKey := getBKey(key)
	if bKey == nil {
		return false
	}
	keyHash := HashFunc(bKey)
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
	sl.Mu.Lock()
	ndIdx := -1
	var foundNd *node
	foundNdIdx := sl.FindNode(keyHash, bKey)
	if foundNdIdx >= 0 {
		foundNd = &sl.Nodes[foundNdIdx]
		if ua == updateActionNone && (foundNd.Expiry < 0 || foundNd.Expiry >= int(time.Now().Unix())) {
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
	bKeyIdx, bKeyLen := 0, len(bKey)
	valIdx, valLen := 0, len(val)
	nd := &sl.Nodes[ndIdx]
	nd.KeyHash = keyHash
	index, offset := nd.Last()
	switch {
	case ua == updateActionNone || ua == updateActionReplace || foundNdIdx < 0:
		if val == nil {
			sl.DelNode(st.slotPool, st.dataPool, ndIdx)
			sl.Mu.Unlock()
			return true
		}
		if !nd.Set(st.slotPool, st.dataPool, bKeyLen+valLen) {
			if foundNdIdx < 0 {
				sl.DelNode(st.slotPool, st.dataPool, ndIdx)
			}
			sl.Mu.Unlock()
			return false
		}
		index = 0
		offset = 0
		nd.Expiry = expiry
	case ua == updateActionAppend:
		if !nd.Alloc(st.slotPool, st.dataPool, valLen) {
			sl.Mu.Unlock()
			return false
		}
		bKeyIdx = bKeyLen
		if expiry >= 0 {
			nd.Expiry = expiry
		}
	}
	for ; bKeyIdx < bKeyLen || valIdx < valLen; index++ {
		data := nd.Datas[index]
		if bKeyIdx < bKeyLen {
			n := copy(data[offset:], bKey[bKeyIdx:])
			offset += n
			bKeyIdx += n
		}
		if offset < len(data) {
			valIdx += copy(data[offset:], val[valIdx:])
		}
		offset = 0
	}
	sl.Mu.Unlock()
	return true
}

func (st *Store) Set(key string, val []byte, expiry int) bool {
	return st.write(key, val, updateActionReplace, expiry)
}

func (st *Store) Put(key string, val []byte, expiry int) bool {
	return st.write(key, val, updateActionNone, expiry)
}

func (st *Store) Append(key string, val []byte, expiry int) bool {
	return st.write(key, val, updateActionAppend, expiry)
}

func (st *Store) Del(key string) bool {
	bKey := getBKey(key)
	if bKey == nil {
		return false
	}
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
