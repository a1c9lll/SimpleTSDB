package aggregators

import (
	"simpletsdb/core"
)

func Min(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total        int
		min0         float64
		lastWindow   int64
		lastFilled   bool
		minnedPoints []*core.Point
	)

	min0 = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastFilled = points[0].Filled

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				min0 = min(min0, pt.Value)
				total++
			} else {
				if lastFilled {
					minnedPoints = append(minnedPoints, points[i-1])
				} else {
					minnedPoints = append(minnedPoints, &core.Point{
						Value:     min0,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				min0 = pt.Value
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			minnedPoints = append(minnedPoints, points[len(points)-1])
		} else {
			minnedPoints = append(minnedPoints, &core.Point{
				Value:     min0,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return minnedPoints
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}