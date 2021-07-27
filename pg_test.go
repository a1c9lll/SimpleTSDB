package main

import (
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.T) {
	log.SetLevel(log.FatalLevel)
	cfg := map[string]string{}
	if err := loadConfig("./config", cfg); err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(cfg["postgres_port"])
	if err != nil {
		t.Fatal(err)
	}
	var pgPassword string
	if p, ok := cfg["postgres_password"]; ok {
		pgPassword = p
	}
	initDB(cfg["postgres_username"], pgPassword, cfg["postgres_host"], port, cfg["postgres_db"]+"_test", cfg["postgres_ssl_mode"])

	deleteMetric("test0")
	deleteMetric("test1")
	deleteMetric("test2")

	err = createMetric("test0", []string{"id", "type"})
	if err != nil {
		t.Fatal(err)
	}
	deleteMetric("test7")

	err = createMetric("test7", []string{"id", "type"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInvalidMetricName(t *testing.T) {
	err := createMetric("a b", []string{})
	if err == nil {
		t.Fatal("expected error")
	}
	if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidTags(t *testing.T) {
	err := createMetric("ab", []string{"c d"})
	if err == nil {
		t.Fatal("expected error")
	}
	if err != errUnsupportedTagName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidMetricNameIninsertPoint(t *testing.T) {
	if err := insertPoint(&insertPointQuery{
		Metric: " a b",
	}); err == nil {
		t.Fatal("expected error")
	} else if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidMetricNameInQuery(t *testing.T) {
	if _, err := queryPoints(&pointsQuery{
		Metric: " a b",
	}); err == nil {
		t.Fatal("expected error")
	} else if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestMetricRequired(t *testing.T) {
	if _, err := queryPoints(&pointsQuery{}); err == nil {
		t.Fatal("expected error")
	} else if err != errMetricRequired {
		t.Fatalf("wrong error: %s", err)
	}

	if err := insertPoint(&insertPointQuery{}); err == nil {
		t.Fatal("expected error")
	} else if err != errMetricRequired {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestMetricExists(t *testing.T) {
	found, err := metricExists("test0")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("metric test0 doesn't exist")
	}
}

func TestInsertPointAndQuery(t *testing.T) {
	for _, pt := range []*insertPointQuery{
		{
			Metric: "test0",
			Tags: map[string]string{
				"id":   "25862",
				"type": "high",
			},
			Point: &point{
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
			Point: &point{
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
			Point: &point{
				Value:     183001199,
				Timestamp: time.Now().UnixNano(),
			},
		},
	} {
		err := insertPoint(pt)
		if err != nil {
			t.Fatal(err)
		}
	}
	points, err := queryPoints(&pointsQuery{
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
}

func TestDeletePoints(t *testing.T) {
	deleteMetric("test9")
	err := createMetric("test9", []string{"id"})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().Add(-time.Minute * 50)
	for i := 0; i < 10; i++ {
		err := insertPoint(&insertPointQuery{
			Metric: "test9",
			Tags: map[string]string{
				"id": "1",
			},
			Point: &point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Minute * 5 * time.Duration(i)).UnixNano(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	err = deletePoints(&deletePointsQuery{
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

	points, err := queryPoints(&pointsQuery{
		Metric: "test9",
		Start:  baseTime.UnixNano(),
		End:    baseTime.Add(time.Minute * 50).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []*point{
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
	err := createMetric("test2", []string{})
	if err != nil {
		t.Fatal(err)
	}
	timestamp := time.Now().UnixNano()
	if err := insertPoint(&insertPointQuery{
		Metric: "test2",
		Point: &point{
			Value:     182599002,
			Timestamp: timestamp,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := insertPoint(&insertPointQuery{
		Metric: "test2",
		Point: &point{
			Value:     182599002,
			Timestamp: timestamp,
		},
	}); err != nil {
		t.Fatal(err)
	}
	pts, err := queryPoints(&pointsQuery{
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
	err := createMetric("test1", []string{"id"})
	if err != nil {
		t.Fatal(err)
	}

	baseTime := time.Now().Add(-time.Minute * 15)
	for i := 0; i < 3; i++ {
		err := insertPoint(&insertPointQuery{
			Metric: "test1",
			Tags: map[string]string{
				"id": "1",
			},
			Point: &point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Minute * 5 * time.Duration(i)).UnixNano(),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	points, err := queryPoints(&pointsQuery{
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
	require.Equal(t, []*point{
		{Value: 0, Timestamp: baseTime.UnixNano(), Window: baseAlignedTime},
		{Value: 1, Timestamp: baseTime.Add(time.Minute * 5).UnixNano(), Window: baseAlignedTime + windowDur},
		{Value: 2, Timestamp: baseTime.Add(time.Minute * 10).UnixNano(), Window: baseAlignedTime + windowDur*2},
	}, points)
}
