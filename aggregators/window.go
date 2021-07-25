package aggregators

import (
	"errors"
	"simpletsdb/core"
	"time"
)

var (
	errCreateEmptyType = errors.New("createEmpty must be boolean")
	errEveryType       = errors.New("every must be boolean in window aggregator")
	errEveryRequired   = errors.New("every field is required in window aggregator")
)

func Window(startTime, endTime int64, options map[string]interface{}, points []*core.Point) ([]*core.Point, error) {
	if _, ok := options["every"]; !ok {
		return nil, errEveryRequired
	}

	var (
		window time.Duration
		err    error
	)
	switch v1 := options["every"].(type) {
	case string:
		window, err = time.ParseDuration(v1)

		if err != nil {
			return nil, err
		}
	default:
		return nil, errEveryType
	}

	var (
		createEmpty bool
	)

	if v, ok := options["createEmpty"]; ok {
		switch v1 := v.(type) {
		case bool:
			createEmpty = v1
		default:
			return nil, errCreateEmptyType
		}
	}

	// We create a new slice of points if we're filling gaps
	if createEmpty {
		newPoints := []*core.Point{}
		windowDur := window.Nanoseconds()
		currentPoint := 0
		startWindowTime, endWindowTime := startTime-startTime%windowDur, endTime-endTime%window.Nanoseconds()
		for windowTime := startWindowTime; windowTime <= endWindowTime; windowTime += windowDur {
			found := false
			for ; currentPoint < len(points); currentPoint++ {
				pt := points[currentPoint]
				if pt.Timestamp >= windowTime && pt.Timestamp < windowTime+window.Nanoseconds() {
					pt.Window = windowTime
					newPoints = append(newPoints, pt)
					found = true
				} else {
					break
				}
			}
			if !found {
				newPoints = append(newPoints, &core.Point{
					Value:     0,
					Timestamp: windowTime,
					Window:    windowTime,
					Filled:    true,
					Null:      true,
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
			if pt.Timestamp >= windowTime && pt.Timestamp < windowTime+window.Nanoseconds() {
				pt.Window = windowTime
			} else {
				break
			}
		}
	}

	return points, nil
}
