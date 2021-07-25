package util

import "time"

func MustParseTime(t string) time.Time {
	t0, err := time.Parse(time.RFC3339Nano, t)
	if err != nil {
		panic(err)
	}
	return t0
}
