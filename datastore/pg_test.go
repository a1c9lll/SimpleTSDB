package datastore

import (
	"simpletsdb/core"
	"simpletsdb/util"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.T) {
	cfg := map[string]string{}
	if err := util.LoadConfig("../config", cfg); err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(cfg["postgres_port"])
	if err != nil {
		t.Fatal(err)
	}
	InitDB(cfg["postgres_username"], cfg["postgres_password"], cfg["postgres_host"], port, cfg["postgres_db"]+"_test", cfg["postgres_ssl_mode"])

	DeleteMetric("test0")
	DeleteMetric("test1")
	DeleteMetric("test2")

	err = CreateMetric("test0", []string{"id", "type"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInvalidMetricName(t *testing.T) {
	err := CreateMetric("a b", []string{})
	if err == nil {
		t.Fatal("expected error")
	}
	if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidTags(t *testing.T) {
	err := CreateMetric("ab", []string{"c d"})
	if err == nil {
		t.Fatal("expected error")
	}
	if err != errUnsupportedTagName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidMetricNameInInsertPoint(t *testing.T) {
	if err := InsertPoint(&core.InsertPointQuery{
		Metric: " a b",
	}); err == nil {
		t.Fatal("expected error")
	} else if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidMetricNameInQuery(t *testing.T) {
	if _, err := QueryPoints(&core.PointsQuery{
		Metric: " a b",
	}); err == nil {
		t.Fatal("expected error")
	} else if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestMetricRequired(t *testing.T) {
	if _, err := QueryPoints(&core.PointsQuery{}); err == nil {
		t.Fatal("expected error")
	} else if err != errMetricRequired {
		t.Fatalf("wrong error: %s", err)
	}

	if err := InsertPoint(&core.InsertPointQuery{}); err == nil {
		t.Fatal("expected error")
	} else if err != errMetricRequired {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestMetricExists(t *testing.T) {
	found, err := MetricExists("test0")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("metric test0 doesn't exist")
	}
}

func TestInsertPointAndQuery(t *testing.T) {
	for _, pt := range []*core.InsertPointQuery{
		{
			Metric: "test0",
			Tags: map[string]string{
				"id":   "25862",
				"type": "high",
			},
			Point: &core.Point{
				Value:     183001000,
				Timestamp: time.Now().UnixNano(),
			},
		},
		{
			Metric: "test0",
			Tags: map[string]string{
				"id":   "25862",
				"type": "low",
			},
			Point: &core.Point{
				Value:     182599002,
				Timestamp: time.Now().UnixNano(),
			},
		},
		{
			Metric: "test0",
			Tags: map[string]string{
				"id":   "25862",
				"type": "high",
			},
			Point: &core.Point{
				Value:     183001199,
				Timestamp: time.Now().UnixNano(),
			},
		},
	} {
		err := InsertPoint(pt)
		if err != nil {
			t.Fatal(err)
		}
	}
	points, err := QueryPoints(&core.PointsQuery{
		Metric: "test0",
		Tags: map[string]string{
			"id":   "25862",
			"type": "high",
		},
		Start: time.Now().Add(-time.Hour * 1).UnixNano(),
		End:   time.Now().UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(points) != 2 {
		t.Fatalf("expected 2 points but got %d", len(points))
	}

	// insert null point
	err = InsertPoint(&core.InsertPointQuery{
		Metric: "test0",
		Tags: map[string]string{
			"id": "24987",
		},
		Point: &core.Point{
			Timestamp: time.Now().UnixNano(),
			Null:      true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	points, err = QueryPoints(&core.PointsQuery{
		Metric: "test0",
		Tags: map[string]string{
			"id": "24987",
		},
		Start: time.Now().Add(-time.Hour * 1).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	type val struct {
		val  float64
		null bool
	}
	vals := []*val{}
	for _, pt := range points {
		vals = append(vals, &val{val: pt.Value, null: pt.Null})
	}
	require.Equal(t, []*val{
		{val: 0, null: true},
	}, vals)
}

func TestDeletePoints(t *testing.T) {
	DeleteMetric("test9")
	err := CreateMetric("test9", []string{"id"})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().Add(-time.Minute * 50)
	for i := 0; i < 10; i++ {
		err := InsertPoint(&core.InsertPointQuery{
			Metric: "test9",
			Tags: map[string]string{
				"id": "1",
			},
			Point: &core.Point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Minute * 5 * time.Duration(i)).UnixNano(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	err = DeletePoints(&core.DeletePointsQuery{
		Metric: "test9",
		Start:  baseTime.Add(time.Minute * 20).UnixNano(),
		End:    baseTime.Add(time.Minute * 30).UnixNano(),
		Tags: map[string]string{
			"id": "1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	points, err := QueryPoints(&core.PointsQuery{
		Metric: "test9",
		Start:  baseTime.UnixNano(),
		End:    baseTime.Add(time.Minute * 50).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []*core.Point{
		{Value: 0, Timestamp: baseTime.UnixNano()},
		{Value: 1, Timestamp: baseTime.Add(time.Minute * 5).UnixNano()},
		{Value: 2, Timestamp: baseTime.Add(time.Minute * 10).UnixNano()},
		{Value: 3, Timestamp: baseTime.Add(time.Minute * 15).UnixNano()},
		{Value: 7, Timestamp: baseTime.Add(time.Minute * 35).UnixNano()},
		{Value: 8, Timestamp: baseTime.Add(time.Minute * 40).UnixNano()},
		{Value: 9, Timestamp: baseTime.Add(time.Minute * 45).UnixNano()},
	}, points)
}

func TestDuplicateInsert(t *testing.T) {
	err := CreateMetric("test2", []string{})
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().UnixNano()
	if err := InsertPoint(&core.InsertPointQuery{
		Metric: "test2",
		Point: &core.Point{
			Value:     182599002,
			Timestamp: timestamp,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := InsertPoint(&core.InsertPointQuery{
		Metric: "test2",
		Point: &core.Point{
			Value:     182599002,
			Timestamp: timestamp,
		},
	}); err != nil {
		t.Fatal(err)
	}
	pts, err := QueryPoints(&core.PointsQuery{
		Metric: "test2",
		Start:  time.Now().Add(-time.Hour).UnixNano(),
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(pts) != 1 {
		t.Fatalf("expected 1 result, got %d", len(pts))
	}
}

func TestWindowAggregator(t *testing.T) {
	err := CreateMetric("test1", []string{"id"})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().Add(-time.Minute * 15)
	for i := 0; i < 3; i++ {
		err := InsertPoint(&core.InsertPointQuery{
			Metric: "test1",
			Tags: map[string]string{
				"id": "1",
			},
			Point: &core.Point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Minute * 5 * time.Duration(i)).UnixNano(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	points, err := QueryPoints(&core.PointsQuery{
		Metric: "test1",
		Tags: map[string]string{
			"id": "1",
		},
		Start: baseTime.UnixNano(),
		Window: map[string]interface{}{
			"every": "5m",
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	startTime := baseTime.UnixNano()
	windowDur := time.Duration(time.Minute * 5).Nanoseconds()
	baseAlignedTime := startTime - startTime%windowDur
	require.Equal(t, []*core.Point{
		{Value: 0, Timestamp: baseTime.UnixNano(), Window: baseAlignedTime},
		{Value: 1, Timestamp: baseTime.Add(time.Minute * 5).UnixNano(), Window: baseAlignedTime + windowDur},
		{Value: 2, Timestamp: baseTime.Add(time.Minute * 10).UnixNano(), Window: baseAlignedTime + windowDur*2},
	}, points)
}
