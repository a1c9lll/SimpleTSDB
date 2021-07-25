package aggregators

import "simpletsdb/core"

func Bucketize(points []*core.Point) [][]*core.Point {
	if len(points) == 0 {
		return [][]*core.Point{}
	}

	var (
		lastWindow int64
		buckets    [][]*core.Point
		idxStart   int
		idxEnd     int
	)

	idxStart = 0
	idxEnd = 1
	lastWindow = points[0].Window

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				idxEnd++
			} else {
				buckets = append(buckets, points[idxStart:idxEnd])
				idxStart = i
				idxEnd = i + 1
			}
			lastWindow = pt.Window
		}
	}

	if idxEnd-idxStart > 0 {
		buckets = append(buckets, points[idxStart:idxEnd])
	}

	return buckets
}
