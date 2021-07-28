package main

import "time"

func mustParseTime(t string) time.Time {
	t0, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return t0
}