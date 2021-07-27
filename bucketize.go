package main

func bucketize(points []*point) [][]*point {
	if len(points) == 0 {
		return [][]*point{}
	}

	var (
		lastWindow int64
		buckets    [][]*point
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
