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
	stats      StoreStats
}

type StoreStats struct {
	KeyCount      int64
	KeyspaceSize  int64
	DataspaceSize int64
	ReqOperCount  int64
	SucOperCount  int64
}

const StoreStatsStr = `Key Count: %d
Keyspace size: %d
Dataspace size: %d
Requested Operation Count: %d
Successful Operation Count: %d
`

type ScanFunc func(key string, size int, index int, data []byte, expiry int) (cont bool)
type GetFunc func(size int, index int, data []byte, expiry int) (cont bool)

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
					bKeyLen := int(nd.Datas[0][0]) + 1
					atomic.AddInt64(&st.stats.KeyspaceSize, -int64(bKeyLen))
					atomic.AddInt64(&st.stats.DataspaceSize, -int64(nd.Size-bKeyLen))
					sl.DelNode(st.slotPool, st.dataPool, j)
				}
			}
			sl.Mu.Unlock()
		}
	}
	tk.Stop()
}

func (st *Store) Stats() (stats StoreStats) {
	stats = st.stats
	return
}

func (st *Store) Scan(f ScanFunc) bool {
	cont := true
	for i := range st.slots {
		sl := &st.slots[i]
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
	return cont
}

func (st *Store) Get(key string, f GetFunc) bool {
	atomic.AddInt64(&st.stats.ReqOperCount, 1)
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
		if ndIdx < 0 {
			sl.Mu.Unlock()
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
			sl.DelNode(st.slotPool, st.dataPool, ndIdx)
			sl.Mu.Unlock()
			atomic.AddInt64(&st.stats.SucOperCount, 1)
			return true
		}
		if !nd.Set(st.slotPool, st.dataPool, bKeyLen+valLen) {
			if foundNdIdx < 0 {
				atomic.AddInt64(&st.stats.KeyCount, -1)
				sl.DelNode(st.slotPool, st.dataPool, ndIdx)
			}
			sl.Mu.Unlock()
			return false
		}
		index = 0
		offset = 0
		nd.Expiry = expiry
		atomic.AddInt64(&st.stats.KeyspaceSize, int64(bKeyLen))
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
	slotIdx := keyHash % len(st.slots)
	sl := &st.slots[slotIdx]
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
	sl.DelNode(st.slotPool, st.dataPool, ndIdx)
	sl.Mu.Unlock()
	atomic.AddInt64(&st.stats.SucOperCount, 1)
	return true
}
