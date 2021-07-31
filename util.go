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

type Item struct {
	fn       func(*sql.DB) error
	done     chan error
	priority int
	index    int
}

type DB struct {
	queue *PriorityQueue
	cond  *sync.Cond
}

func (db *DB) Query(priority int, fn func(*sql.DB) error) error {
	item := &Item{
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

type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}
