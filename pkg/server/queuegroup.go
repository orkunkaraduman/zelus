package server

import "time"

type queueGroup struct {
	addr              string
	nodeSetQueue      *SetQueue
	masterSetQueue    *SetQueue
	masterPutQueue    *SetQueue
	masterAppendQueue *SetQueue
	remove            bool
}

func newQueueGroup(addr string, connectTimeout, pingTimeout time.Duration, connectRetryCount int, maxLen, maxSize int) (q *queueGroup) {
	q = &queueGroup{
		addr:              addr,
		nodeSetQueue:      NewSetQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "SET", true),
		masterSetQueue:    NewSetQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "SET", false),
		masterPutQueue:    NewSetQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "PUT", false),
		masterAppendQueue: NewSetQueue(addr, connectTimeout, pingTimeout, connectRetryCount, maxLen, maxSize, "APPEND", false),
	}
	return
}

func (q *queueGroup) Close() {
	q.nodeSetQueue.Close()
	q.masterSetQueue.Close()
	q.masterPutQueue.Close()
	q.masterAppendQueue.Close()
}
