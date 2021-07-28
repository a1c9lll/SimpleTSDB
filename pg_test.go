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

	_, err = session.Exec("DELETE FROM simpletsdb_metrics WHERE true")
	if err != nil {
		t.Fatal(err)
	}
}

func TestInvalidMetricNameIninsertPoint(t *testing.T) {
	if err := insertPoints([]*insertPointQuery{
		{Metric: " a b"},
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
}

func TestInsertPointAndQuery(t *testing.T) {
	pts := []*insertPointQuery{
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
	}
	err := insertPoints(pts)
	if err != nil {
		t.Fatal(err)
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
	baseTime := time.Now().Add(-time.Minute * 50)
	pts := []*insertPointQuery{}
	for i := 0; i < 10; i++ {
		pts = append(pts, &insertPointQuery{
			Metric: "test9",
			Tags: map[string]string{
				"id": "1",
			},
			Point: &point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Minute * 5 * time.Duration(i)).UnixNano(),
			},
		})
	}

	err := insertPoints(pts)
	if err != nil {
		t.Fatal(err)
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
	timestamp := time.Now().UnixNano()
	insertPts := []*insertPointQuery{
		{
			Metric: "test2",
			Point: &point{
				Value:     182599002,
				Timestamp: timestamp,
			},
		},
	}
	err := insertPoints(insertPts)
	if err != nil {
		t.Fatal(err)
	}

	insertPts = []*insertPointQuery{
		{
			Metric: "test2",
			Point: &point{
				Value:     182599002,
				Timestamp: timestamp,
			},
		},
	}
	err = insertPoints(insertPts)
	if err != nil {
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
	baseTime := time.Now().Add(-time.Minute * 15)
	insertPts := []*insertPointQuery{}
	for i := 0; i < 3; i++ {
		insertPts = append(insertPts, &insertPointQuery{
			Metric: "test1",
			Tags: map[string]string{
				"id": "1",
			},
			Point: &point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Minute * 5 * time.Duration(i)).UnixNano(),
			},
		})
	}

	err := insertPoints(insertPts)
	if err != nil {
		t.Fatal(err)
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
