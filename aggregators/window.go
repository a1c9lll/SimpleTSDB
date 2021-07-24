package aggregators

import (
	"errors"
	"simpletsdb/core"
	"time"
)

var (
	errFillGapsType        = errors.New("fillGaps must be boolean")
	errEveryRequired       = errors.New("'every' field is required for windowing")
	errFillValueType       = errors.New("fillValue must be int or float")
	errFillUsePreviousType = errors.New("fillUsePrevious must be boolean")
	errFillValueRequired   = errors.New("fillValue is required for windowing")
)

func Window(startTime, endTime int64, options map[string]interface{}, points []*core.Point) ([]*core.Point, error) {
	if _, ok := options["every"]; !ok {
		return nil, errEveryRequired
	}

	window, err := time.ParseDuration(options["every"].(string))

	if err != nil {
		return nil, err
	}

	var (
		fillGaps    bool
		fillValue   = float64(-1)
		usePrevious bool
	)
	if v, ok := options["fillGaps"]; ok {
		switch v1 := v.(type) {
		case bool:
			fillGaps = v1
		default:
			return nil, errFillGapsType
		}
		var (
			fillValueFound bool
		)
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
				return nil, errFillValueType
			}
			fillValueFound = true
		}
		if v3, ok := options["fillUsePrevious"]; ok {
			switch v1 := v3.(type) {
			case bool:
				usePrevious = v1
			default:
				return nil, errFillUsePreviousType
			}
		}

		if !fillValueFound {
			return nil, errFillValueRequired
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
				var v float64
				if usePrevious {
					// fill value is used here if there is no previous value
					// TODO: add query for filling previous value from
					//       last value of previous time interval
					if currentPoint > 0 {
						v = points[currentPoint-1].Value
					} else {
						v = fillValue
					}
				} else {
					v = fillValue
				}
				newPoints = append(newPoints, &core.Point{
					Value:     v,
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
