package aggregators

import (
	"simpletsdb/core"
)

func First(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total       int
		first       *core.Point
		lastWindow  int64
		lastFilled  bool
		firstPoints []*core.Point
	)

	first = points[0]
	lastWindow = points[0].Window
	total = 1
	lastFilled = points[0].Filled

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				total++
			} else {
				if lastFilled {
					firstPoints = append(firstPoints, points[i-1])
				} else {
					firstPoints = append(firstPoints, &core.Point{
						Value:     first.Value,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				first = pt
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			firstPoints = append(firstPoints, points[len(points)-1])
		} else {
			firstPoints = append(firstPoints, &core.Point{
				Value:     first.Value,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return firstPoints
}
