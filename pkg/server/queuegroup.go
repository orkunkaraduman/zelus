package server

import "time"

type queueGroup struct {
	addr              string
	nodeSetQueue      *queue
	masterGetQueue    *queue
	masterSetQueue    *queue
	masterPutQueue    *queue
	masterAppendQueue *queue
	remove            bool
}

func newQueueGroup(addr string, connectTimeout, pingTimeout time.Duration, connectRetryCount int, maxLen, maxSize int) (q *queueGroup) {
	q = &queueGroup{
		addr:              addr,
		nodeSetQueue:      newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "SET", true),
		masterGetQueue:    newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "GET", false),
		masterSetQueue:    newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "SET", false),
		masterPutQueue:    newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "PUT", false),
		masterAppendQueue: newQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "APPEND", false),
	}
	return
}

func (q *queueGroup) Close() {
	q.masterGetQueue.Close()
	q.nodeSetQueue.Close()
	q.masterSetQueue.Close()
	q.masterPutQueue.Close()
	q.masterAppendQueue.Close()
}
