package server

import "time"

type queueGroup struct {
	addr              string
	nodeGetQueue      *queue
	nodeSetQueue      *queue
	masterSetQueue    *queue
	masterPutQueue    *queue
	masterAppendQueue *queue
	remove            bool
}

func newQueueGroup(addr string, connectTimeout, pingTimeout time.Duration, connectRetryCount int, maxLen, maxSize int) (q *queueGroup) {
	q = &queueGroup{
		addr:              addr,
		nodeGetQueue:      newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "GET", true),
		nodeSetQueue:      newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "SET", true),
		masterSetQueue:    newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "SET", false),
		masterPutQueue:    newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "PUT", false),
		masterAppendQueue: newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "APPEND", false),
	}
	return
}

func (q *queueGroup) Close() {
	q.nodeGetQueue.Close()
	q.nodeSetQueue.Close()
	q.masterSetQueue.Close()
	q.masterPutQueue.Close()
	q.masterAppendQueue.Close()
}
