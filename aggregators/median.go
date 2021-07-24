package aggregators

import (
	"simpletsdb/core"
	"sort"
)

func Median(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	medianPoints := []*core.Point{}
	buckets := Bucketize(points)
	for _, bucket := range buckets {
		pts := core.Points(bucket)
		if pts[0].Filled {
			medianPoints = append(medianPoints, pts[0])
			continue
		}
		sort.Sort(pts)
		var median float64
		if len(pts)%2 == 1 {
			median = pts[(len(pts)+1)/2-1].Value
		} else {
			median = (pts[(len(pts)/2-1)].Value + pts[len(pts)/2].Value) / 2
		}
		medianPoints = append(medianPoints, &core.Point{
			Value:     median,
			Timestamp: pts[0].Window,
			Window:    pts[0].Window,
		})
	}

	return medianPoints
}
