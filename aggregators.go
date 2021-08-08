package main

import (
	"errors"
	"math"
	"sort"
	"time"
)

var (
	errCountNullPointsType = errors.New("countNullPoints must be boolean")
	errStdDevOptionInvalid = errors.New("valid options for stddev mode are population and sample")
	errStdDevModeType      = errors.New("stddev mode must be string")
	errFillValueRequired   = errors.New("fillValue must be set for fill aggregator")
	errUsePreviousType     = errors.New("usePrevious must be boolean")
	errFillValueType       = errors.New("fillValue must be int or float")
	errCreateEmptyType     = errors.New("createEmpty must be boolean")
	errEveryType           = errors.New("every must be boolean in window aggregator")
	errEveryRequired       = errors.New("every field is required in window aggregator")
)

func window(startTime, endTime int64, options map[string]interface{}, points []*point) ([]*point, error) {
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
		newPoints := []*point{}
		windowDur := window.Nanoseconds()
		currentPoint := 0
		startWindowTime, endWindowTime := startTime-startTime%windowDur, endTime-endTime%windowDur
		for windowTime := startWindowTime; windowTime <= endWindowTime; windowTime += windowDur {
			found := false
			for ; currentPoint < len(points); currentPoint++ {
				pt := points[currentPoint]
				if pt.Timestamp >= windowTime && pt.Timestamp < windowTime+windowDur {
					pt.Window = windowTime
					newPoints = append(newPoints, pt)
					found = true
				} else {
					break
				}
			}
			if !found {
				newPoints = append(newPoints, &point{
					Value:     0,
					Timestamp: windowTime,
					Window:    windowTime,
					Null:      true,
				})
			}
		}
		return newPoints, nil
	}

	// We're not filling gaps
	windowDur := window.Nanoseconds()
	currentPoint := 0
	startWindowTime, endWindowTime := startTime-startTime%windowDur, endTime-endTime%windowDur

	for windowTime := startWindowTime; windowTime <= endWindowTime; windowTime += windowDur {
		for ; currentPoint < len(points); currentPoint++ {
			pt := points[currentPoint]
			if pt.Timestamp >= windowTime && pt.Timestamp < windowTime+windowDur {
				pt.Window = windowTime
			} else {
				break
			}
		}
	}

	return points, nil
}

func bucketize(points []*point) [][]*point {
	if len(points) == 0 {
		return [][]*point{}
	}

	var (
		lastWindow int64
		buckets    [][]*point
		idxStart   int
		idxEnd     int
	)

	idxStart = 0
	idxEnd = 1
	lastWindow = points[0].Window

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				idxEnd++
			} else {
				buckets = append(buckets, points[idxStart:idxEnd])
				idxStart = i
				idxEnd = i + 1
			}
			lastWindow = pt.Window
		}
	}

	if idxEnd-idxStart > 0 {
		buckets = append(buckets, points[idxStart:idxEnd])
	}

	return buckets
}

func last(points []*point) []*point {
	if len(points) == 0 {
		return points
	}

	var (
		total      int
		last       *point
		lastWindow int64
		lastNull   bool
		lastPoints []*point
	)

	last = points[0]
	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				last = pt
				total++
			} else {
				if lastNull {
					lastPoints = append(lastPoints, points[i-1])
				} else {
					lastPoints = append(lastPoints, &point{
						Value:     last.Value,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				last = pt
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			lastPoints = append(lastPoints, points[len(points)-1])
		} else {
			lastPoints = append(lastPoints, &point{
				Value:     last.Value,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return lastPoints
}

func first(points []*point) []*point {
	if len(points) == 0 {
		return points
	}

	var (
		total       int
		first       *point
		lastWindow  int64
		lastNull    bool
		firstPoints []*point
	)

	first = points[0]
	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				total++
			} else {
				if lastNull {
					firstPoints = append(firstPoints, points[i-1])
				} else {
					firstPoints = append(firstPoints, &point{
						Value:     first.Value,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				first = pt
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			firstPoints = append(firstPoints, points[len(points)-1])
		} else {
			firstPoints = append(firstPoints, &point{
				Value:     first.Value,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return firstPoints
}

func count(options map[string]interface{}, points []*point) ([]*point, error) {
	if len(points) == 0 {
		return points, nil
	}

	var countNullPoints bool

	if v, ok := options["countNullPoints"]; ok {
		switch v1 := v.(type) {
		case bool:
			countNullPoints = v1
		default:
			return nil, errCountNullPointsType
		}
	}

	var (
		total         int
		lastWindow    int64
		lastNull      bool
		countedPoints []*point
	)

	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				total++
			} else {
				if lastNull {
					if countNullPoints {
						countedPoints = append(countedPoints, &point{
							Value:     1,
							Timestamp: points[i-1].Timestamp,
							Window:    points[i-1].Timestamp,
							Null:      false,
						})
					} else {
						countedPoints = append(countedPoints, points[i-1])
					}
				} else {
					countedPoints = append(countedPoints, &point{
						Value:     float64(total),
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			if countNullPoints {
				countedPoints = append(countedPoints, &point{
					Value:     1,
					Timestamp: points[len(points)-1].Timestamp,
					Window:    points[len(points)-1].Timestamp,
				})
			} else {
				countedPoints = append(countedPoints, points[len(points)-1])
			}
		} else {
			countedPoints = append(countedPoints, &point{
				Value:     float64(total),
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return countedPoints, nil
}

func max(points []*point) []*point {
	if len(points) == 0 {
		return points
	}

	var (
		total       int
		max1        float64
		lastWindow  int64
		lastNull    bool
		maxedPoints []*point
	)

	max1 = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				max1 = max0(max1, pt.Value)
				total++
			} else {
				if lastNull {
					maxedPoints = append(maxedPoints, points[i-1])
				} else {
					maxedPoints = append(maxedPoints, &point{
						Value:     max1,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				max1 = pt.Value
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			maxedPoints = append(maxedPoints, points[len(points)-1])
		} else {
			maxedPoints = append(maxedPoints, &point{
				Value:     max1,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return maxedPoints
}

func min(points []*point) []*point {
	if len(points) == 0 {
		return points
	}

	var (
		total        int
		min1         float64
		lastWindow   int64
		lastNull     bool
		minnedPoints []*point
	)

	min1 = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				min1 = min0(min1, pt.Value)
				total++
			} else {
				if lastNull {
					minnedPoints = append(minnedPoints, points[i-1])
				} else {
					minnedPoints = append(minnedPoints, &point{
						Value:     min1,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				min1 = pt.Value
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			minnedPoints = append(minnedPoints, points[len(points)-1])
		} else {
			minnedPoints = append(minnedPoints, &point{
				Value:     min1,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return minnedPoints
}

func mean(points []*point) []*point {
	if len(points) == 0 {
		return points
	}

	var (
		total          int
		sum            float64
		lastWindow     int64
		lastNull       bool
		averagedPoints []*point
	)

	sum = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				sum += pt.Value
				total++
			} else {
				if lastNull {
					averagedPoints = append(averagedPoints, points[i-1])
				} else {
					averagedPoints = append(averagedPoints, &point{
						Value:     sum / float64(total),
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				sum = pt.Value
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			averagedPoints = append(averagedPoints, points[len(points)-1])
		} else {
			averagedPoints = append(averagedPoints, &point{
				Value:     sum / float64(total),
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return averagedPoints
}

func median(points0 []*point) []*point {
	if len(points0) == 0 {
		return points0
	}

	buckets := bucketize(points0)
	medianPoints := make([]*point, len(buckets))

	for i, bucket := range buckets {
		pts := points(bucket)
		if pts[0].Null {
			medianPoints[i] = pts[0]
			continue
		}
		sort.Sort(pts)
		var median float64
		if len(pts)%2 == 1 {
			median = pts[(len(pts)+1)/2-1].Value
		} else {
			median = (pts[(len(pts)/2-1)].Value + pts[len(pts)/2].Value) / 2
		}
		medianPoints[i] = &point{
			Value:     median,
			Timestamp: pts[0].Window,
			Window:    pts[0].Window,
		}
	}

	return medianPoints
}

func sum(points []*point) []*point {
	if len(points) == 0 {
		return points
	}

	var (
		total        int
		sum          float64
		lastWindow   int64
		lastNull     bool
		summedPoints []*point
	)

	sum = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastNull = points[0].Null

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				sum += pt.Value
				total++
			} else {
				if lastNull {
					summedPoints = append(summedPoints, points[i-1])
				} else {
					summedPoints = append(summedPoints, &point{
						Value:     sum,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				sum = pt.Value
				total = 1
				lastNull = pt.Null
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastNull {
			summedPoints = append(summedPoints, points[len(points)-1])
		} else {
			summedPoints = append(summedPoints, &point{
				Value:     sum,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return summedPoints
}

func mode(points []*point) []*point {
	buckets := bucketize(points)
	modePoints := make([]*point, len(buckets))

	for i, bucket := range buckets {
		var (
			modes  []*point
			mode   *point
			counts = map[float64]int{}
			max    = -1
		)
		for _, n := range bucket {
			count := 0
			if v, ok := counts[n.Value]; ok {
				count = v + 1
			} else {
				count = 1
			}
			counts[n.Value] = count

			if count > max {
				max = count
				mode = n
			}
		}

		for _, v := range counts {
			if v > 1 {
				modes = append(modes, mode)
			}
		}

		if len(modes) == 0 || len(modes) > 1 {
			modePoints[i] = &point{
				Value:     0,
				Timestamp: bucket[0].Window,
				Window:    bucket[0].Window,
				Null:      true,
			}
		} else {
			modePoints[i] = &point{
				Value:     mode.Value,
				Timestamp: mode.Window,
				Window:    mode.Window,
			}
		}
	}

	return modePoints
}

func stddev(options map[string]interface{}, points []*point) ([]*point, error) {
	if len(points) == 0 {
		return points, nil
	}

	var (
		sampleStdDev = true
	)

	if v, ok := options["mode"]; ok {
		switch v1 := v.(type) {
		case string:
			if v1 != "population" && v1 != "sample" {
				return nil, errStdDevOptionInvalid
			}
			if v1 == "population" {
				sampleStdDev = false
			}
		default:
			return nil, errStdDevModeType
		}
	}

	buckets := bucketize(points)
	stdDevedPoints := make([]*point, len(buckets))

	for i, bucket := range buckets {
		if bucket[0].Null {
			stdDevedPoints[i] = bucket[0]
			continue
		}
		if len(bucket) == 1 && sampleStdDev {
			stdDevedPoints[i] = &point{
				Value:     0,
				Timestamp: bucket[0].Window,
				Window:    bucket[0].Window,
				Null:      true,
			}
			continue
		}

		var (
			sum    float64
			stdDev float64
		)

		for _, pt := range bucket {
			sum += pt.Value
		}

		mean := sum / float64(len(bucket))

		for _, pt := range bucket {
			stdDev += math.Pow(pt.Value-mean, 2)
		}

		var lenM float64
		if sampleStdDev {
			lenM = float64(len(bucket) - 1)
		} else {
			lenM = float64(len(bucket))
		}
		stdDevedPoints[i] = &point{
			Value:     math.Sqrt(stdDev / lenM),
			Timestamp: bucket[0].Window,
			Window:    bucket[0].Window,
		}
	}

	return stdDevedPoints, nil
}

// fillValue is required even if usePrevious is set incase
// the first point is null
func fill(options map[string]interface{}, points []*point) ([]*point, error) {
	if len(points) == 0 {
		return points, nil
	}

	var (
		usePrevious bool
		fillValue   = float64(-1)
	)

	if v, ok := options["usePrevious"]; ok {
		switch v1 := v.(type) {
		case bool:
			usePrevious = v1
		default:
			return nil, errUsePreviousType
		}
	}

	if v, ok := options["fillValue"]; ok {
		switch v1 := v.(type) {
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
	} else {
		return nil, errFillValueRequired
	}

	if points[0].Null {
		points[0].Value = fillValue
		points[0].Null = false
	}

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Null {
				if usePrevious {
					pt.Value = points[i-1].Value
				} else {
					pt.Value = fillValue
				}
				pt.Null = false
			}
		}
	}

	return points, nil
}
