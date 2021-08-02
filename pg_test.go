package main

import (
	"database/sql"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/require"
)

var (
	db0 *dbConn
)

func TestMain(t *testing.T) {
	log.SetLevel(log.FatalLevel)
	cfg := map[string]string{}
	if err := loadConfig("./config/simpletsdb-dev.conf", cfg); err != nil {
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
	db0 = initDB(cfg["postgres_username"], pgPassword, cfg["postgres_host"], port, cfg["postgres_db"]+"_test", cfg["postgres_ssl_mode"], 1)

	err = db0.Query(0, func(db *sql.DB) error {
		_, err = db.Exec("DELETE FROM simpletsdb_metrics WHERE true")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDownsample(t *testing.T) {
	// insert some points
	baseTime := mustParseTime("2000-01-01T00:00:00Z").Add(-time.Hour)
	ipts := []*insertPointQuery{}
	for i := 0; i < 60; i++ {
		ipts = append(ipts, &insertPointQuery{
			Metric: "test09z",
			Tags: map[string]string{
				"id": "2",
			},
			Point: &point{
				Value:     float64(i),
				Timestamp: baseTime.Add(time.Duration(i) * time.Minute).UnixNano(),
			},
		})
	}

	if err := insertPoints(db0, ipts); err != nil {
		t.Fatal(err)
	}

	if err := downsample(db0, &downsampler{
		Metric:      "test09z",
		OutMetric:   "test09z_15m",
		RunEvery:    "15m",
		RunEveryDur: time.Minute * 15,
		Query: &downsampleQuery{
			Tags: map[string]string{
				"id": "2",
			},
			Window: map[string]interface{}{
				"every": "15m",
			},
			Aggregators: []*aggregatorQuery{
				{Name: "mean"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidMetricNameIninsertPoint(t *testing.T) {
	if err := insertPoints(db0, []*insertPointQuery{
		{Metric: " a b"},
	}); err == nil {
		t.Fatal("expected error")
	} else if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInvalidMetricNameInQuery(t *testing.T) {
	if _, err := queryPoints(db0, &pointsQuery{
		Metric: " a b",
	}); err == nil {
		t.Fatal("expected error")
	} else if err != errUnsupportedMetricName {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestMetricRequired(t *testing.T) {
	if _, err := queryPoints(db0, &pointsQuery{}); err == nil {
		t.Fatal("expected error")
	} else if err != errMetricRequired {
		t.Fatalf("wrong error: %s", err)
	}
}

func TestInsertPointAndQuery(t *testing.T) {
	baseTime := time.Now().Add(-time.Second)
	pts := []*insertPointQuery{
		{
			Metric: "test0",
			Tags: map[string]string{
				"id":   "25862",
				"type": "high",
			},
			Point: &point{
				Value:     183001000,
				Timestamp: baseTime.UnixNano(),
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
				Timestamp: baseTime.Add(time.Millisecond).Local().UnixNano(),
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
				Timestamp: baseTime.Add(time.Millisecond * 2).Local().UnixNano(),
			},
		},
	}
	err := insertPoints(db0, pts)
	if err != nil {
		t.Fatal(err)
	}
	points, err := queryPoints(db0, &pointsQuery{
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

	err := insertPoints(db0, pts)
	if err != nil {
		t.Fatal(err)
	}

	err = deletePoints(db0, &deletePointsQuery{
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

	points, err := queryPoints(db0, &pointsQuery{
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
	err := insertPoints(db0, insertPts)
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
	err = insertPoints(db0, insertPts)
	if err != nil {
		t.Fatal(err)
	}

	pts, err := queryPoints(db0, &pointsQuery{
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

	err := insertPoints(db0, insertPts)
	if err != nil {
		t.Fatal(err)
	}

	points, err := queryPoints(db0, &pointsQuery{
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
