package aggregators

import (
	"simpletsdb/core"
)

func Last(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total      int
		last       *core.Point
		lastWindow int64
		lastFilled bool
		lastPoints []*core.Point
	)

	last = points[0]
	lastWindow = points[0].Window
	total = 1
	lastFilled = points[0].Filled

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				last = pt
				total++
			} else {
				if lastFilled {
					lastPoints = append(lastPoints, points[i-1])
				} else {
					lastPoints = append(lastPoints, &core.Point{
						Value:     last.Value,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				last = pt
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			lastPoints = append(lastPoints, points[len(points)-1])
		} else {
			lastPoints = append(lastPoints, &core.Point{
				Value:     last.Value,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return lastPoints
}
