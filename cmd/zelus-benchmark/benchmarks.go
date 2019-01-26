package main

import (
	"sync/atomic"
	"time"

	"github.com/orkunkaraduman/zelus/pkg/client"
)

type benchmarkResult struct {
	err      error
	count    int64
	duration time.Duration
}

func set(cl *client.Client, keys []string, multi int, datasize int, brCh chan benchmarkResult, count *int64) {
	var err error
	k1 := make([]string, 0, multi)
	v1 := make([][]byte, 0, multi)
	startTm := time.Now()
	i, j := 0, 0
	for i < len(keys) {
		k1 = k1[:0]
		v1 = v1[:0]
		for j = 0; j < multi && i < len(keys); j++ {
			n := (i * datasize) % len(lipsum)
			if n+datasize > len(lipsum) {
				n = 0
			}
			k1 = append(k1, keys[i])
			v1 = append(v1, lipsum[n:n+datasize])
			i++
		}
		var k []string
		k, err = cl.Set(k1, v1)
		atomic.AddInt64(count, int64(len(k)))
		if err != nil {
			i -= j
			break
		}
		if len(k) != len(k1) {
			i -= j
			i += len(k)
			break
		}
	}
	brCh <- benchmarkResult{
		err:      err,
		count:    int64(i),
		duration: time.Now().Sub(startTm),
	}
}

func get(cl *client.Client, keys []string, multi int, datasize int, brCh chan benchmarkResult, count *int64) {
	var err error
	k1 := make([]string, 0, multi)
	k := make([]string, 0, multi)
	startTm := time.Now()
	i, j := 0, 0
	for i < len(keys) {
		k1 = k1[:0]
		for j = 0; j < multi && i < len(keys); j++ {
			k1 = append(k1, keys[i])
			i++
		}
		k = k[:0]
		err = cl.Get(k1, func(key string, val []byte) {
			k = append(k, key)
		})
		atomic.AddInt64(count, int64(len(k)))
		if err != nil {
			i -= j
			break
		}
		if len(k) != len(k1) {
			i -= j
			i += len(k)
			break
		}
	}
	brCh <- benchmarkResult{
		err:      err,
		count:    int64(i),
		duration: time.Now().Sub(startTm),
	}
}

func del(cl *client.Client, keys []string, multi int, datasize int, brCh chan benchmarkResult, count *int64) {
	var err error
	k1 := make([]string, 0, multi)
	startTm := time.Now()
	i, j := 0, 0
	for i < len(keys) {
		k1 = k1[:0]
		for j = 0; j < multi && i < len(keys); j++ {
			k1 = append(k1, keys[i])
			i++
		}
		var k []string
		k, err = cl.Del(k1)
		atomic.AddInt64(count, int64(len(k)))
		if err != nil {
			i -= j
			break
		}
		if len(k) != len(k1) {
			i -= j
			i += len(k)
			break
		}
	}
	brCh <- benchmarkResult{
		err:      err,
		count:    int64(i),
		duration: time.Now().Sub(startTm),
	}
}
