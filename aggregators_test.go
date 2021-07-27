package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWindow(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 1, Timestamp: baseTime.UnixNano()},
		{Value: 2, Timestamp: baseTime.Add(time.Minute).UnixNano()},
		{Value: 3, Timestamp: baseTime.Add(time.Minute * 3).UnixNano()},
		{Value: 4, Timestamp: baseTime.Add(time.Minute * 4).UnixNano()},
		{Value: 5, Timestamp: baseTime.Add(time.Minute * 6).UnixNano()},
	}

	pts, err := window(baseTime.Add(-time.Minute).UnixNano(), baseTime.Add(time.Minute*8).UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	windowDur := time.Duration(time.Minute).Nanoseconds()
	baseTimeNano := baseTime.UnixNano()
	alignedStartTime := baseTimeNano - baseTimeNano%windowDur

	require.Equal(t, []*point{
		{Value: 0, Null: true, Timestamp: baseTime.Add(-time.Minute).UnixNano(), Window: alignedStartTime - windowDur},
		{Value: 1, Timestamp: baseTime.UnixNano(), Window: alignedStartTime},
		{Value: 2, Timestamp: baseTime.Add(time.Minute).UnixNano(), Window: alignedStartTime + windowDur},
		{Value: 0, Null: true, Timestamp: alignedStartTime + time.Duration(time.Minute*2).Nanoseconds(), Window: alignedStartTime + windowDur*2},
		{Value: 3, Timestamp: baseTime.Add(time.Minute * 3).UnixNano(), Window: alignedStartTime + windowDur*3},
		{Value: 4, Timestamp: baseTime.Add(time.Minute * 4).UnixNano(), Window: alignedStartTime + windowDur*4},
		{Value: 0, Null: true, Timestamp: alignedStartTime + time.Duration(time.Minute*5).Nanoseconds(), Window: alignedStartTime + windowDur*5},
		{Value: 5, Timestamp: baseTime.Add(time.Minute * 6).UnixNano(), Window: alignedStartTime + windowDur*6},
		{Value: 0, Null: true, Timestamp: alignedStartTime + time.Duration(time.Minute*7).Nanoseconds(), Window: alignedStartTime + windowDur*7},
		{Value: 0, Null: true, Timestamp: alignedStartTime + time.Duration(time.Minute*8).Nanoseconds(), Window: alignedStartTime + windowDur*8},
	}, pts)

}

func TestMean(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = mean(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		34.8, 64, 1758.6666666666667, 0,
	}, vals)
}

func TestMean2(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z").Add(-1 * time.Minute)
	pts := []*point{
		{Value: 42, Timestamp: baseTime.Add(30 * time.Second).UnixNano()},
	}

	pts, err := window(baseTime.UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every": "30s",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = mean(pts)

	if len(pts) != 1 {
		t.Fatal()
	}

	value := pts[0].Value
	if value != 42 {
		t.Fatal()
	}
}

func TestMean3(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z").Add(-time.Minute)
	pts := []*point{}

	pts, err := window(baseTime.UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = mean(pts)

	if len(pts) != 1 {
		t.Fatal()
	}

	if !pts[0].Null {
		t.Fatal()
	}

	if pts[0].Timestamp != pts[0].Window {
		t.Fatal()
	}
}

func TestSum(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = sum(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		174, 128, 5276, 0,
	}, vals)
}

func TestMin(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = min(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		10, 55, 999, 0,
	}, vals)
}

func TestMax(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = max(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		98, 73, 2940, 0,
	}, vals)
}

func TestMax2(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 1).UnixNano()},
	}

	pts, err := window(baseTime.Add(-time.Minute*1).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = max(pts)

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
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 4).UnixNano()},
		{Value: 12, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 13, Timestamp: baseTime.Add(-time.Minute).UnixNano()},
		{Value: 14, Timestamp: baseTime.UnixNano()},
	}

	pts, err := window(baseTime.Add(-time.Minute*4).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = max(pts)

	vals := []float64{}
	for _, pt := range pts {
		if pt.Timestamp != pt.Window {
			t.Fatal()
		}
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		11, 12, 0, 13, 14,
	}, vals)
}

func TestCount(t *testing.T) {
	// test without counting null points
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = count(map[string]interface{}{}, pts)
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
		5, 2, 3, 0,
	}, vals)

	// test with counting null points
	pts = []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err = window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = count(map[string]interface{}{
		"countNullPoints": true,
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
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every":       "1m",
		"createEmpty": true,
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = first(pts)

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
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = last(pts)

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

func TestBucketize(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	buckets := bucketize(pts)

	vals := [][]float64{}
	for i, bucket := range buckets {
		vals = append(vals, []float64{})
		for _, pt := range bucket {
			vals[i] = append(vals[i], pt.Value)
		}
	}

	require.Equal(t, [][]float64{
		{11, 25, 30, 10, 98},
		{73, 55},
		{999, 1337, 2940},
	}, vals)

	// test bucketize with one point
	pts = []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
	}

	pts, err = window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	buckets = bucketize(pts)

	vals = [][]float64{}
	for i, bucket := range buckets {
		vals = append(vals, []float64{})
		for _, pt := range bucket {
			vals[i] = append(vals[i], pt.Value)
		}
	}

	require.Equal(t, [][]float64{
		{11},
	}, vals)
}

func TestMedian(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = median(pts)

	vals := []float64{}
	for _, pt := range pts {
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		25, 64, 1337,
	}, vals)
}

func TestMode(t *testing.T) {
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 4},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts = mode(pts)

	type val struct {
		val  float64
		null bool
	}

	vals := []*val{}
	for _, pt := range pts {
		vals = append(vals, &val{pt.Value, pt.Null})
	}

	require.Equal(t, []*val{
		{val: 11, null: false},
		{val: 0, null: true},
		{val: 0, null: true},
	}, vals)
}

func TestStdDev(t *testing.T) {
	// population mode
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts := []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err := window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = stddev(map[string]interface{}{
		"mode": "population",
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	vals := []float64{}
	for _, pt := range pts {
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		32.541665599658536, 9, 846.6492124185133,
	}, vals)

	// sample mode
	pts = []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 25, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
		{Value: 30, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 1},
		{Value: 10, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 2},
		{Value: 98, Timestamp: baseTime.Add(-time.Minute*3).UnixNano() + 3},
		{Value: 73, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 1},
		{Value: 55, Timestamp: baseTime.Add(-time.Minute*2).UnixNano() + 2},
		{Value: 999, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 1},
		{Value: 1337, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 2},
		{Value: 2940, Timestamp: baseTime.Add(-time.Minute*1).UnixNano() + 3},
	}

	pts, err = window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = stddev(map[string]interface{}{
		"mode": "sample",
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	vals = []float64{}
	for _, pt := range pts {
		vals = append(vals, pt.Value)
	}

	require.Equal(t, []float64{
		36.38268819095148, 12.727922061357855, 1036.9292807773024,
	}, vals)

	// sample mode with 1 point
	pts = []*point{
		{Value: 11, Timestamp: baseTime.Add(-time.Minute * 3).UnixNano()},
	}

	pts, err = window(baseTime.Add(-time.Minute*3).UnixNano(), baseTime.Add(-time.Minute*1).UnixNano()+3, map[string]interface{}{
		"every": "1m",
	}, pts)

	if err != nil {
		t.Fatal(err)
	}

	pts, err = stddev(map[string]interface{}{
		"mode": "sample",
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	if len(pts) != 1 {
		t.Fatal()
	}
	if !pts[0].Null {
		t.Fatal()
	}
}

func TestFill(t *testing.T) {
	pts := []*point{}

	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	pts, err := window(baseTime.Add(-time.Second*30).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "5s",
		"createEmpty": true,
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	pts, err = fill(map[string]interface{}{
		"fillValue": -1,
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	type val struct {
		val  float64
		null bool
	}
	vals := []*val{}
	for _, pt := range pts {
		vals = append(vals, &val{pt.Value, pt.Null})
	}
	require.Equal(t, []*val{
		{val: -1, null: false},
		{val: -1, null: false},
		{val: -1, null: false},
		{val: -1, null: false},
		{val: -1, null: false},
		{val: -1, null: false},
		{val: -1, null: false},
	}, vals)

	// with usePrevious
	pts = []*point{
		{Value: 42, Timestamp: baseTime.Add(-time.Second * 30).UnixNano()},
	}

	pts, err = window(baseTime.Add(-time.Second*30).UnixNano(), baseTime.UnixNano(), map[string]interface{}{
		"every":       "5s",
		"createEmpty": true,
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	pts, err = fill(map[string]interface{}{
		"usePrevious": true,
		"fillValue":   -1,
	}, pts)
	if err != nil {
		t.Fatal(err)
	}

	vals = []*val{}
	for _, pt := range pts {
		vals = append(vals, &val{pt.Value, pt.Null})
	}

	require.Equal(t, []*val{
		{val: 42, null: false},
		{val: 42, null: false},
		{val: 42, null: false},
		{val: 42, null: false},
		{val: 42, null: false},
		{val: 42, null: false},
		{val: 42, null: false},
	}, vals)
}
