package aggregators

import "simpletsdb/core"

func Bucketize(points []*core.Point) [][]*core.Point {
	if len(points) == 0 {
		return [][]*core.Point{}
	}

	var (
		total         int
		currentBucket []*core.Point
		lastWindow    int64
		buckets       [][]*core.Point
	)

	currentBucket = append(currentBucket, points[0])
	lastWindow = points[0].Window
	total = 1

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				currentBucket = append(currentBucket, pt)
				total++
			} else {
				buckets = append(buckets, currentBucket)

				currentBucket = []*core.Point{
					pt,
				}
				total = 1
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		buckets = append(buckets, currentBucket)
	}

	return buckets
}
