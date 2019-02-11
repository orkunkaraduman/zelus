package store

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/malloc"
)

type Store struct {
	buckets          [][]slot
	bucketsMu        sync.RWMutex
	slotCount        int
	lhN              int
	lhL              int
	lhS              int
	slotPool         *malloc.Pool
	dataPool         *malloc.Pool
	disposerCloseCh  chan struct{}
	disposerClosedCh chan struct{}
	stats            StoreStats
}

type StoreStats struct {
	KeyCount      int64
	KeyspaceSize  int64
	DataspaceSize int64
	ReqOperCount  int64
	SucOperCount  int64
}

type ScanFunc func(key string, size int, index int, data []byte, expiry int) (cont bool)
type GetFunc func(size int, index int, data []byte, expiry int) (cont bool)

type updateAction int

const (
	updateActionNone = updateAction(iota)
	updateActionReplace
	updateActionAppend
)

func New(bucketSize int, memorySize int) (st *Store) {
	if bucketSize <= 0 || memorySize <= 0 {
		return
	}
	p := malloc.AllocPool(memorySize)
	st = &Store{
		buckets:          make([][]slot, 0, 64),
		lhN:              bucketSize,
		slotPool:         p,
		dataPool:         p,
		disposerCloseCh:  make(chan struct{}, 1),
		disposerClosedCh: make(chan struct{}),
	}
	st.buckets = append(st.buckets, make([]slot, st.lhN))
	st.slotCount += st.lhN
	go st.disposer()
	return
}

func (st *Store) Close() {
	select {
	case st.disposerCloseCh <- struct{}{}:
	default:
	}
	<-st.disposerClosedCh
	st.slotPool.Close()
	st.dataPool.Close()
}

func (st *Store) disposer() {
	tk := time.NewTicker(60 * time.Second)
	for {
		done := false
		select {
		case <-tk.C:
			st.bucketsMu.RLock()
			for _, bu := range st.buckets {
				for i := range bu {
					if len(st.disposerCloseCh) != 0 {
						break
					}
					sl := &bu[i]
					sl.Mu.Lock()
					for j := 0; j < len(sl.Nodes); j++ {
						if len(st.disposerCloseCh) != 0 {
							break
						}
						nd := &sl.Nodes[j]
						if nd.KeyHash >= 0 && nd.Expiry >= 0 && nd.Expiry < int(time.Now().Unix()) {
							bKeyLen := int(nd.Datas[0][0]) + 1
							atomic.AddInt64(&st.stats.KeyCount, -1)
							atomic.AddInt64(&st.stats.KeyspaceSize, -int64(bKeyLen))
							atomic.AddInt64(&st.stats.DataspaceSize, -int64(nd.Size-bKeyLen))
							sl.FreeNode(j, st.slotPool, st.dataPool)
						}
					}
					sl.Mu.Unlock()
				}
			}
			st.bucketsMu.RUnlock()
		case <-st.disposerCloseCh:
			done = true
		}
		if done {
			break
		}
	}
	tk.Stop()
	close(st.disposerClosedCh)
}

func (st *Store) getSlot(h int) *slot {
	_, bucketNo, bucketOffset := lhSlotLocation(st.lhN, st.lhL, st.lhS, h)
	return &st.buckets[bucketNo][bucketOffset]
}

func (st *Store) expand(n int) {
	st.bucketsMu.Lock()
	for m := 0; m < n; m++ {
		L, S := st.lhL, st.lhS
		L1 := L + 1
		if S == 0 {
			st.buckets = append(st.buckets, make([]slot, 0, st.lhN*(1<<uint(L))))
		}
		idx := len(st.buckets[L1])
		st.buckets[L1] = st.buckets[L1][:idx+1]
		sl1 := st.getSlot(S)
		sl2 := &st.buckets[L1][idx]

		st.lhS++
		if st.lhS >= st.lhN*(1<<uint(st.lhL)) {
			st.lhL++
			st.lhS = 0
		}
		st.slotCount++

		for ndIdx1 := 0; ndIdx1 < len(sl1.Nodes); ndIdx1++ {
			nd1 := &sl1.Nodes[ndIdx1]
			if nd1.KeyHash < 0 {
				continue
			}
			offset1, _, _ := lhSlotLocation(st.lhN, L, S, nd1.KeyHash)
			offset2, _, _ := lhSlotLocation(st.lhN, st.lhL, st.lhS, nd1.KeyHash)
			if offset1 == offset2 {
				continue
			}
			ndIdx2 := sl2.NewNode(st.slotPool)
			nd2 := &sl2.Nodes[ndIdx2]
			*nd2 = *nd1
			sl1.DelNode(ndIdx1, st.slotPool)
		}
	}
	st.bucketsMu.Unlock()
}

func (st *Store) Stats() (stats StoreStats) {
	stats = st.stats
	return
}

func (st *Store) Scan(f ScanFunc) (cont bool) {
	cont = true
	st.bucketsMu.RLock()
	for _, bu := range st.buckets {
		for i := range bu {
			sl := &bu[i]
			sl.Mu.Lock()
			for j := 0; j < len(sl.Nodes); j++ {
				nd := &sl.Nodes[j]
				if nd.KeyHash >= 0 && (nd.Expiry < 0 || nd.Expiry >= int(time.Now().Unix())) {
					bKeyLen := int(nd.Datas[0][0]) + 1
					bKey := make([]byte, 0, bKeyLen)
					key := ""
					p := bKeyLen
					valIdx, valLen := 0, nd.Size-p
					for index := 0; valIdx < valLen; index++ {
						data := nd.Datas[index]
						n, r := 0, len(data)
						if p != 0 {
							if r > p {
								r = p
							}
							m := len(bKey)
							bKey = bKey[:m+r]
							copy(bKey[m:], data[n:n+r])
							n += r
							p -= r
						}
						if p == 0 {
							if key == "" {
								key = string(bKey[1 : 1+bKey[0]])
							}
							d := data[n:]
							cont = f(key, valLen, valIdx, d, nd.Expiry)
							valIdx += len(d)
							if !cont {
								break
							}
						}
					}
				}
				if !cont {
					break
				}
			}
			sl.Mu.Unlock()
			if !cont {
				break
			}
		}
	}
	st.bucketsMu.RUnlock()
	return
}

func (st *Store) Get(key string, f GetFunc) bool {
	atomic.AddInt64(&st.stats.ReqOperCount, 1)
	bKey := getBKey(key)
	if bKey == nil {
		return false
	}
	keyHash := HashFunc(bKey)
	st.bucketsMu.RLock()
	sl := st.getSlot(keyHash)
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
	cont := true
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
			cont = f(valLen, valIdx, d, nd.Expiry)
			valIdx += len(d)
			if !cont {
				break
			}
		}
	}
	sl.Mu.Unlock()
	st.bucketsMu.RUnlock()
	atomic.AddInt64(&st.stats.SucOperCount, 1)
	return true
}

func (st *Store) write(key string, val []byte, ua updateAction, expiry int, f GetFunc) bool {
	atomic.AddInt64(&st.stats.ReqOperCount, 1)
	bKey := getBKey(key)
	if bKey == nil {
		return false
	}
	keyHash := HashFunc(bKey)
	st.bucketsMu.RLock()
	sl := st.getSlot(keyHash)
	sl.Mu.Lock()
	ndIdx := -1
	var foundNd *node
	foundNdIdx := sl.FindNode(keyHash, bKey)
	if foundNdIdx >= 0 {
		foundNd = &sl.Nodes[foundNdIdx]
		if ua == updateActionNone && (foundNd.Expiry < 0 || foundNd.Expiry >= int(time.Now().Unix())) {
			sl.Mu.Unlock()
			st.bucketsMu.RUnlock()
			return false
		}
		ndIdx = foundNdIdx
	} else {
		ndIdx = sl.NewNode(st.slotPool)
		if ndIdx < 0 {
			sl.Mu.Unlock()
			st.bucketsMu.RUnlock()
			return false
		}
		atomic.AddInt64(&st.stats.KeyCount, 1)
	}
	bKeyIdx, bKeyLen := 0, len(bKey)
	valIdx, valLen := 0, len(val)
	nd := &sl.Nodes[ndIdx]
	nd.KeyHash = keyHash
	index, offset := nd.Last()
	switch {
	case ua == updateActionNone || ua == updateActionReplace || foundNdIdx < 0:
		if foundNdIdx >= 0 {
			atomic.AddInt64(&st.stats.KeyspaceSize, -int64(bKeyLen))
			atomic.AddInt64(&st.stats.DataspaceSize, -int64(nd.Size-bKeyLen))
		}
		if val == nil {
			atomic.AddInt64(&st.stats.KeyCount, -1)
			sl.FreeNode(ndIdx, st.slotPool, st.dataPool)
			sl.Mu.Unlock()
			st.bucketsMu.RUnlock()
			atomic.AddInt64(&st.stats.SucOperCount, 1)
			return true
		}
		if !nd.Set(st.slotPool, st.dataPool, bKeyLen+valLen) {
			if foundNdIdx < 0 {
				atomic.AddInt64(&st.stats.KeyCount, -1)
				sl.FreeNode(ndIdx, st.slotPool, st.dataPool)
			}
			sl.Mu.Unlock()
			st.bucketsMu.RUnlock()
			return false
		}
		index = 0
		offset = 0
		nd.Expiry = expiry
		atomic.AddInt64(&st.stats.KeyspaceSize, int64(bKeyLen))
	case ua == updateActionAppend:
		if !nd.Alloc(st.slotPool, st.dataPool, valLen) {
			sl.Mu.Unlock()
			st.bucketsMu.RUnlock()
			return false
		}
		bKeyIdx = bKeyLen
		if expiry >= 0 {
			nd.Expiry = expiry
		}
	}
	atomic.AddInt64(&st.stats.DataspaceSize, int64(valLen))
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
	if f != nil {
		p := bKeyLen
		valIdx, valLen := 0, nd.Size-p
		cont := true
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
				cont = f(valLen, valIdx, d, nd.Expiry)
				valIdx += len(d)
				if !cont {
					break
				}
			}
		}
	}
	sl.Mu.Unlock()

	kc := int(st.stats.KeyCount)
	if kc > st.slotCount {
		go st.expand(kc - st.slotCount)
	}

	st.bucketsMu.RUnlock()
	atomic.AddInt64(&st.stats.SucOperCount, 1)
	return true
}

func (st *Store) Set(key string, val []byte, expiry int, f GetFunc) bool {
	return st.write(key, val, updateActionReplace, expiry, f)
}

func (st *Store) Put(key string, val []byte, expiry int, f GetFunc) bool {
	return st.write(key, val, updateActionNone, expiry, f)
}

func (st *Store) Append(key string, val []byte, expiry int, f GetFunc) bool {
	return st.write(key, val, updateActionAppend, expiry, f)
}

func (st *Store) Del(key string) bool {
	atomic.AddInt64(&st.stats.ReqOperCount, 1)
	bKey := getBKey(key)
	bKeyLen := len(bKey)
	if bKey == nil {
		return false
	}
	keyHash := HashFunc(bKey)
	st.bucketsMu.RLock()
	sl := st.getSlot(keyHash)
	sl.Mu.Lock()
	ndIdx := sl.FindNode(keyHash, bKey)
	if ndIdx < 0 {
		sl.Mu.Unlock()
		return false
	}
	nd := &sl.Nodes[ndIdx]
	atomic.AddInt64(&st.stats.KeyCount, -1)
	atomic.AddInt64(&st.stats.KeyspaceSize, -int64(bKeyLen))
	atomic.AddInt64(&st.stats.DataspaceSize, -int64(nd.Size-bKeyLen))
	sl.FreeNode(ndIdx, st.slotPool, st.dataPool)
	sl.Mu.Unlock()
	st.bucketsMu.RUnlock()
	atomic.AddInt64(&st.stats.SucOperCount, 1)
	return true
}
