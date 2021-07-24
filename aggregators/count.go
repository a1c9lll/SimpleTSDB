package aggregators

import (
	"errors"
	"simpletsdb/core"
)

var (
	errCountFilledPointsType = errors.New("countFilledPoints must be boolean")
)

func Count(options map[string]interface{}, points []*core.Point) ([]*core.Point, error) {
	if len(points) == 0 {
		return points, nil
	}

	var countFilledPoints bool

	if v, ok := options["countFilledPoints"]; ok {
		switch v1 := v.(type) {
		case bool:
			countFilledPoints = v1
		default:
			return nil, errCountFilledPointsType
		}
	}

	var (
		total         int
		lastWindow    int64
		lastFilled    bool
		countedPoints []*core.Point
	)

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
					if countFilledPoints {
						countedPoints = append(countedPoints, &core.Point{
							Value:     1,
							Timestamp: points[i-1].Timestamp,
							Window:    points[i-1].Timestamp,
							Filled:    false,
						})
					} else {
						countedPoints = append(countedPoints, points[i-1])
					}
				} else {
					countedPoints = append(countedPoints, &core.Point{
						Value:     float64(total),
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			if countFilledPoints {
				countedPoints = append(countedPoints, &core.Point{
					Value:     1,
					Timestamp: points[len(points)-1].Timestamp,
					Window:    points[len(points)-1].Timestamp,
					Filled:    false,
				})
			} else {
				countedPoints = append(countedPoints, points[len(points)-1])
			}
		} else {
			countedPoints = append(countedPoints, &core.Point{
				Value:     float64(total),
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return countedPoints, nil
}
