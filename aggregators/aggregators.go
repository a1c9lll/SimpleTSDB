package aggregators

// A lot of code is duplicated in this file for efficiency.
// Another approach would be to use the bucketize function
// on the data. It would make each of these fns more concise.
// However, bucketizing isn't necessary for many of these
// aggregators.
import (
	"errors"
	"math"
	"simpletsdb/core"
	"sort"
)

var (
	errCountFilledPointsType = errors.New("countFilledPoints must be boolean")
	errNoModeValueType       = errors.New("noModeValue must be int or float")
	errStdDevOptionInvalid   = errors.New("valid options for stddev mode are population and sample")
	errStdDevModeType        = errors.New("stddev mode must be string")
	errNoStdDevValueType     = errors.New("noStdDevValue must be int or float")
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

func Max(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total       int
		max0        float64
		lastWindow  int64
		lastFilled  bool
		maxedPoints []*core.Point
	)

	max0 = points[0].Value
	lastWindow = points[0].Window
	total = 1
	lastFilled = points[0].Filled

	if len(points) > 1 {
		for i := 1; i < len(points); i++ {
			pt := points[i]
			if pt.Window == lastWindow {
				max0 = max(max0, pt.Value)
				total++
			} else {
				if lastFilled {
					maxedPoints = append(maxedPoints, points[i-1])
				} else {
					maxedPoints = append(maxedPoints, &core.Point{
						Value:     max0,
						Timestamp: lastWindow,
						Window:    lastWindow,
					})
				}
				max0 = pt.Value
				total = 1
				lastFilled = pt.Filled
			}
			lastWindow = pt.Window
		}
	}

	if total > 0 {
		if lastFilled {
			maxedPoints = append(maxedPoints, points[len(points)-1])
		} else {
			maxedPoints = append(maxedPoints, &core.Point{
				Value:     max0,
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return maxedPoints
}

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

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func Mean(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	var (
		total          int
		sum            float64
		lastWindow     int64
		lastFilled     bool
		averagedPoints []*core.Point
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
					averagedPoints = append(averagedPoints, points[i-1])
				} else {
					averagedPoints = append(averagedPoints, &core.Point{
						Value:     sum / float64(total),
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
			averagedPoints = append(averagedPoints, points[len(points)-1])
		} else {
			averagedPoints = append(averagedPoints, &core.Point{
				Value:     sum / float64(total),
				Timestamp: lastWindow,
				Window:    lastWindow,
			})
		}
	}

	return averagedPoints
}

func Median(points []*core.Point) []*core.Point {
	if len(points) == 0 {
		return points
	}

	buckets := Bucketize(points)
	medianPoints := make([]*core.Point, len(buckets))

	for i, bucket := range buckets {
		pts := core.Points(bucket)
		if pts[0].Filled {
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
		medianPoints[i] = &core.Point{
			Value:     median,
			Timestamp: pts[0].Window,
			Window:    pts[0].Window,
		}
	}

	return medianPoints
}

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

func Mode(options map[string]interface{}, points []*core.Point) ([]*core.Point, error) {
	var (
		noModeValue    float64
		useNoModeValue bool
	)
	if v, ok := options["noModeValue"]; ok {
		switch v1 := v.(type) {
		case float64:
			noModeValue = v1
		case float32:
			noModeValue = float64(v1)
		case int:
			noModeValue = float64(v1)
		case int32:
			noModeValue = float64(v1)
		case int64:
			noModeValue = float64(v1)
		default:
			return nil, errNoModeValueType
		}
		useNoModeValue = true
	}

	buckets := Bucketize(points)
	modePoints := make([]*core.Point, len(buckets))

	for i, bucket := range buckets {
		var (
			modes  []*core.Point
			mode   *core.Point
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
			modePoints[i] = &core.Point{
				Value:     noModeValue,
				Timestamp: bucket[0].Window,
				Window:    bucket[0].Window,
				Null:      !useNoModeValue,
			}
		} else {
			modePoints[i] = &core.Point{
				Value:     mode.Value,
				Timestamp: mode.Window,
				Window:    mode.Window,
			}
		}
	}

	return modePoints, nil
}

func StdDev(options map[string]interface{}, points []*core.Point) ([]*core.Point, error) {
	if len(points) == 0 {
		return points, nil
	}

	var (
		sampleStdDev       = true
		noStdDevValue      float64
		noStdDevValueFound bool
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

	if v, ok := options["noStdDevValue"]; ok {
		switch v1 := v.(type) {
		case int:
			noStdDevValue = float64(v1)
		case int32:
			noStdDevValue = float64(v1)
		case int64:
			noStdDevValue = float64(v1)
		case float32:
			noStdDevValue = float64(v1)
		case float64:
			noStdDevValue = v1
		default:
			return nil, errNoStdDevValueType
		}
		noStdDevValueFound = true
	}

	buckets := Bucketize(points)
	stdDevedPoints := make([]*core.Point, len(buckets))

	for i, bucket := range buckets {
		if bucket[0].Filled {
			stdDevedPoints[i] = bucket[0]
			continue
		}
		if len(bucket) == 1 && sampleStdDev {
			stdDevedPoints[i] = &core.Point{
				Value:     noStdDevValue,
				Timestamp: bucket[0].Window,
				Window:    bucket[0].Window,
				Null:      !noStdDevValueFound,
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
		stdDevedPoints[i] = &core.Point{
			Value:     math.Sqrt(stdDev / lenM),
			Timestamp: bucket[0].Window,
			Window:    bucket[0].Window,
		}
	}

	return stdDevedPoints, nil
}
