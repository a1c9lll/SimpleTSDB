package aggregators

import "simpletsdb/core"

func Sum(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total        int
		sum          float64
		lastWindow   int64
		lastFilled   bool
		summedPoints []*core.Point
	)

	sum = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastFilled = points[0].Filled

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				sum += pt.Value
				total++
			} else {
				if lastFilled {
					summedPoints = append(summedPoints, points[i-1])
				} else {
					summedPoints = append(summedPoints, &core.Point{
						Value:     sum,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				sum = pt.Value
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			summedPoints = append(summedPoints, points[len(points)-1])
		} else {
			summedPoints = append(summedPoints, &core.Point{
				Value:     sum,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return summedPoints
}
