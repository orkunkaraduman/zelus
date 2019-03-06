package dynlist

type DynList struct {
	n, l, s int
	lf      LessFunc
	buckets [][]interface{}
}

func New(n int, lf LessFunc) (dl *DynList) {
	if n <= 0 {
		return
	}
	dl = &DynList{
		n:       n,
		lf:      lf,
		buckets: make([][]interface{}, 0, 64),
	}
	return
}

func (dl *DynList) location(offset int) (bucketNo, bucketOffset int) {
	if offset < 0 {
		return -1, -1
	}
	m, n := 0, offset/dl.n+1
	for n != 0 {
		n >>= 1
		m++
	}
	bucketNo = m - 1
	bucketOffset = offset - dl.n*(1<<uint(bucketNo)-1)
	return
}

func (dl *DynList) Len() int {
	return dl.n*(1<<uint(dl.l)-1) + dl.s
}

func (dl *DynList) Less(i, j int) bool {
	bucketNo1, bucketOffset1 := dl.location(i)
	bucketNo2, bucketOffset2 := dl.location(j)
	return dl.lf(dl.buckets[bucketNo1][bucketOffset1], dl.buckets[bucketNo2][bucketOffset2])
}

func (dl *DynList) Swap(i, j int) {
	bucketNo1, bucketOffset1 := dl.location(i)
	bucketNo2, bucketOffset2 := dl.location(j)
	dl.buckets[bucketNo1][bucketOffset1], dl.buckets[bucketNo2][bucketOffset2] =
		dl.buckets[bucketNo2][bucketOffset2], dl.buckets[bucketNo1][bucketOffset1]
}

func (dl *DynList) Push(x interface{}) {
	if dl.s == 0 {
		dl.buckets = append(dl.buckets, make([]interface{}, 0, dl.n*(1<<uint(dl.l))))
	}
	dl.buckets[dl.l] = append(dl.buckets[dl.l], x)
	dl.s++
	if dl.s >= dl.n*(1<<uint(dl.l)) {
		dl.l++
		dl.s = 0
	}
}

func (dl *DynList) Pop() interface{} {
	if dl.s == 0 {
		dl.l--
		dl.s = dl.n*(1<<uint(dl.l)) - 1
	} else {
		dl.s--
	}
	x := dl.buckets[dl.l][dl.s]
	dl.buckets[dl.l] = dl.buckets[dl.l][:dl.s]
	if dl.s == 0 {
		dl.buckets[dl.l] = nil
		dl.buckets = dl.buckets[:dl.l]
	}
	return x
}

func (dl *DynList) Get(i int) interface{} {
	bucketNo, bucketOffset := dl.location(i)
	return dl.buckets[bucketNo][bucketOffset]
}

func (dl *DynList) Set(i int, x interface{}) {
	bucketNo, bucketOffset := dl.location(i)
	dl.buckets[bucketNo][bucketOffset] = x
}
