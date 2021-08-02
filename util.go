package main

import (
	"container/heap"
	"database/sql"
	"sync"
	"sync/atomic"
)

func min1(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type AtomicBool struct {
	flag int32
}

func (b *AtomicBool) Set(value bool) {
	var i int32 = 0
	if value {
		i = 1
	}
	atomic.StoreInt32(&(b.flag), int32(i))
}

func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&(b.flag)) != 0
}

type item struct {
	fn       func(*sql.DB) error
	done     chan error
	priority int
	index    int
}

type dbConn struct {
	queue *priorityQueue
	cond  *sync.Cond
}

func (db *dbConn) Query(priority int, fn func(*sql.DB) error) error {
	item := &item{
		fn:       fn,
		priority: priority,
		done:     make(chan error),
	}

	db.cond.L.Lock()
	heap.Push(db.queue, item)
	db.cond.Signal()
	db.cond.L.Unlock()

	err := <-item.done
	return err
}

type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
