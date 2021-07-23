package aggregators

import (
	"errors"
	"simpletsdb/core"
	"time"
)

func Window(startTime, endTime int64, options map[string]interface{}, points []*core.Point) ([]*core.Point, error) {
	if _, ok := options["every"]; !ok {
		return nil, errors.New("'every' field is required for windowing")
	}

	window, err := time.ParseDuration(options["every"].(string))

	if err != nil {
		return nil, err
	}

	fillGaps := false
	fillValue := float64(-1)
	if v, ok := options["fillGaps"]; ok {
		switch v1 := v.(type) {
		case bool:
			fillGaps = v1
		default:
			return nil, errors.New("fillGaps must be boolean")
		}
		if v2, ok := options["fillValue"]; ok {
			switch v1 := v2.(type) {
			case int:
				fillValue = float64(v1)
			case int32:
				fillValue = float64(v1)
			case int64:
				fillValue = float64(v1)
			case float32:
				fillValue = float64(v1)
			case float64:
				fillValue = v1
			default:
				return nil, errors.New("fillValue must be int or float")
			}
		}
	}

	// We create a new slice of points if we're filling gaps
	if fillGaps {
		newPoints := []*core.Point{}
		windowDur := window.Nanoseconds()
		currentPoint := 0
		startWindowTime, endWindowTime := startTime-startTime%windowDur, endTime-endTime%window.Nanoseconds()
		for windowTime := startWindowTime; windowTime <= endWindowTime; windowTime += windowDur {
			found := false
			for ; currentPoint < len(points); currentPoint++ {
				pt := points[currentPoint]
				if pt.Timestamp >= windowTime && pt.Timestamp <= windowTime+window.Nanoseconds() {
					pt.Window = windowTime
					newPoints = append(newPoints, pt)
					found = true
				} else {
					break
				}
			}
			if !found {
				newPoints = append(newPoints, &core.Point{
					Value:     fillValue,
					Timestamp: windowTime,
					Window:    windowTime,
					Filled:    true,
				})
			}
		}
		return newPoints, nil
	}

	// We're not filling gaps
	windowDur := window.Nanoseconds()
	currentPoint := 0
	startWindowTime, endWindowTime := startTime-startTime%windowDur, endTime-endTime%window.Nanoseconds()

	for windowTime := startWindowTime; windowTime <= endWindowTime; windowTime += windowDur {
		for ; currentPoint < len(points); currentPoint++ {
			pt := points[currentPoint]
			if pt.Timestamp >= windowTime && pt.Timestamp <= windowTime+window.Nanoseconds() {
				pt.Window = windowTime
			} else {
				break
			}
		}
	}

	return points, nil
}
