package aggregators

import "simpletsdb/core"

func Average(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total          int
		sum            float64
		lastWindow     int64
		averagedPoints []*core.Point
	)

	sum = points[0].Value
	lastWindow = points[0].Window
	total = 1

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				sum += pt.Value
				total++
			} else {
				averagedPoints = append(averagedPoints, &core.Point{
					Value:     sum / float64(total),
					Timestamp: lastWindow,
					Window:    lastWindow,
				})
				sum = pt.Value
				total = 1
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		averagedPoints = append(averagedPoints, &core.Point{
			Value:     sum / float64(total),
			Timestamp: lastWindow,
			Window:    lastWindow,
		})
	}

	return averagedPoints
}
