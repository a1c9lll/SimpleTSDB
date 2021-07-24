package aggregators

import (
	"simpletsdb/core"
)

func Max(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total       int
		max0        float64
		lastWindow  int64
		lastFilled  bool
		maxedPoints []*core.Point
	)

	max0 = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastFilled = points[0].Filled

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				max0 = max(max0, pt.Value)
				total++
			} else {
				if lastFilled {
					maxedPoints = append(maxedPoints, points[i-1])
				} else {
					maxedPoints = append(maxedPoints, &core.Point{
						Value:     max0,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				max0 = pt.Value
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			maxedPoints = append(maxedPoints, points[len(points)-1])
		} else {
			maxedPoints = append(maxedPoints, &core.Point{
				Value:     max0,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return maxedPoints
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
