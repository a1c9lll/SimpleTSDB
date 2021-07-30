package main

import "sync/atomic"

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
