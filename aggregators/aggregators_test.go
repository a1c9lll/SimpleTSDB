package aggregators

import (
	"simpletsdb/core"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWindow(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 1, Timestamp: baseDate.UnixNano()},
		{Value: 2, Timestamp: baseDate.Add(time.Minute).UnixNano()},
		{Value: 3, Timestamp: baseDate.Add(time.Minute * 3).UnixNano()},
		{Value: 4, Timestamp: baseDate.Add(time.Minute * 4).UnixNano()},
		{Value: 5, Timestamp: baseDate.Add(time.Minute * 6).UnixNano()},
	}

	pts, err := Window(baseDate.Add(-time.Minute).UnixNano(), baseDate.Add(time.Minute*8).UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	windowDur := time.Duration(time.Minute).Nanoseconds()
	baseTime := baseDate.UnixNano()
	alignedStartTime := baseTime - baseTime%windowDur

	require.Equal(t, []*core.Point{
		{Value: -1, Timestamp: alignedStartTime - time.Duration(time.Minute).Nanoseconds(), Window: alignedStartTime - windowDur, Filled: true},
		{Value: 1, Timestamp: baseDate.UnixNano(), Window: alignedStartTime, Filled: false},
		{Value: 2, Timestamp: baseDate.Add(time.Minute).UnixNano(), Window: alignedStartTime + windowDur, Filled: false},
		{Value: -1, Timestamp: alignedStartTime + time.Duration(time.Minute*2).Nanoseconds(), Window: alignedStartTime + windowDur*2, Filled: true},
		{Value: 3, Timestamp: baseDate.Add(time.Minute * 3).UnixNano(), Window: alignedStartTime + windowDur*3, Filled: false},
		{Value: 4, Timestamp: baseDate.Add(time.Minute * 4).UnixNano(), Window: alignedStartTime + windowDur*4, Filled: false},
		{Value: -1, Timestamp: alignedStartTime + time.Duration(time.Minute*5).Nanoseconds(), Window: alignedStartTime + windowDur*5, Filled: true},
		{Value: 5, Timestamp: baseDate.Add(time.Minute * 6).UnixNano(), Window: alignedStartTime + windowDur*6, Filled: false},
		{Value: -1, Timestamp: alignedStartTime + time.Duration(time.Minute*7).Nanoseconds(), Window: alignedStartTime + windowDur*7, Filled: true},
		{Value: -1, Timestamp: alignedStartTime + time.Duration(time.Minute*8).Nanoseconds(), Window: alignedStartTime + windowDur*8, Filled: true},
	}, pts)
}

func TestWindowUsePrevious(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 1, Timestamp: baseDate.Add(-time.Minute).UnixNano()},
		{Value: 2, Timestamp: baseDate.UnixNano()},
		{Value: 3, Timestamp: baseDate.Add(time.Minute * 2).UnixNano()},
		{Value: 4, Timestamp: baseDate.Add(time.Minute * 3).UnixNano()},
		{Value: 5, Timestamp: baseDate.Add(time.Minute * 5).UnixNano()},
		{Value: 6, Timestamp: baseDate.Add(time.Minute * 8).UnixNano()},
	}

	pts, err := Window(baseDate.Add(-2*time.Minute).UnixNano(), baseDate.Add(time.Minute*8).UnixNano(), map[string]interface{}{
		"every":           "1m",
		"fillGaps":        true,
		"fillValue":       -1,
		"fillUsePrevious": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}
	values := []float64{}
	for _, pt := range pts {
		values = append(values, pt.Value)
	}

	require.Equal(t, []float64{
		-1, 1, 2, 2, 3, 4, 4, 5, 5, 5, 6,
	}, values)
}

func TestMean(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Mean(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		34.8, 64, 1758.6666666666667, -1,
	}, vals)
}

func TestMean2(t *testing.T) {
	baseTime := time.Now().Add(-1 * time.Minute)
	pts := []*core.Point{
		{Value: 42, Timestamp: baseTime.Add(30 * time.Second).UnixNano()},
	}

	pts, err := Window(baseTime.UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every": "30s",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Mean(pts)

	if len(pts) != 1 {
		t.Fatal()
	}

	value := pts[0].Value
	if value != 42 {
		t.Fatal()
	}
}

func TestMean3(t *testing.T) {
	baseTime := time.Now().Add(-time.Minute)
	pts := []*core.Point{}

	pts, err := Window(baseTime.UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Mean(pts)

	if len(pts) != 1 {
		t.Fatal()
	}

	if !pts[0].Filled {
		t.Fatal()
	}

	if pts[0].Timestamp != pts[0].Window {
		t.Fatal()
	}
}

func TestSum(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Sum(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		174, 128, 5276, -1,
	}, vals)
}

func TestMin(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Min(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		10, 55, 999, -1,
	}, vals)
}

func TestMax(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Max(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		98, 73, 2940, -1,
	}, vals)
}

func TestMax2(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 1).UnixNano()},
	}

	pts, err := Window(baseDate.Add(-time.Minute*1).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Max(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		11,
	}, vals)
}

func TestMax3(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 4).UnixNano()},
		{Value: 12, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 13, Timestamp: baseDate.Add(-time.Minute).UnixNano()},
		{Value: 14, Timestamp: baseDate.UnixNano()},
	}

	pts, err := Window(baseDate.Add(-time.Minute*4).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Max(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		11, 12, -1, 13, 14,
	}, vals)
}

func TestCount(t *testing.T) {
	// test without counting filled points
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = Count(map[string]interface{}{}, pts)
	if err != nil {
		t.Fatal(err)
	}

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		5, 2, 3, -1,
	}, vals)

	// test with counting filled points
	pts = []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err = Window(baseDate.Add(-time.Minute*3).UnixNano(), time.Now().UnixNano(), map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = Count(map[string]interface{}{
		"countFilledPoints": true,
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	vals = []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		5, 2, 3, 1,
	}, vals)
}

func TestFirst(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), baseDate.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = First(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		11, 73, 999,
	}, vals)
}

func TestLast(t *testing.T) {
	baseDate := time.Now()
	pts := []*core.Point{
		{Value: 11, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseDate.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseDate.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseDate.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseDate.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := Window(baseDate.Add(-time.Minute*3).UnixNano(), baseDate.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every":     "1m",
		"fillGaps":  true,
		"fillValue": -1,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = Last(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		98, 55, 2940,
	}, vals)
}
