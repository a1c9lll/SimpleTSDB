package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricExistsHandler(t *testing.T) {
	// test invalid request
	req := httptest.NewRequest("GET", "/metric_exists", nil)
	w := httptest.NewRecorder()

	metricExistsHandler(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal()
	}

	resp0 := &serverError{}
	if err := json.NewDecoder(resp.Body).Decode(resp0); err != nil {
		t.Fatal(err)
	}

	if resp0.Error != "metric is required" {
		t.Fatal()
	}

	// test valid request and exists=true
	req = httptest.NewRequest("GET", "/metric_exists?metric=test7", nil)
	w = httptest.NewRecorder()

	metricExistsHandler(w, req, nil)

	resp = w.Result()

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal()
	}

	resp1 := &metricExistsResponse{}
	if err := json.NewDecoder(resp.Body).Decode(resp1); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &metricExistsResponse{
		Exists: true,
	}, resp1)

	// test valid request and exists=false
	req = httptest.NewRequest("GET", "/metric_exists?metric=test999x", nil)
	w = httptest.NewRecorder()

	metricExistsHandler(w, req, nil)

	resp = w.Result()

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal()
	}

	resp2 := &metricExistsResponse{}
	if err := json.NewDecoder(resp.Body).Decode(resp2); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &metricExistsResponse{
		Exists: false,
	}, resp2)
}

func TestCreateMetricHandler(t *testing.T) {
	// test without content-type set
	body := &bytes.Buffer{}
	err := json.NewEncoder(body).Encode(&createMetricRequest{
		Metric: "test0",
		Tags:   []string{"id", "type"},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/create_metric", body)
	w := httptest.NewRecorder()

	createMetricHandler(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}

	serverError := &serverError{}
	json.NewDecoder(resp.Body).Decode(serverError)
	if serverError.Error != "content-type not set" {
		t.Fatal()
	}

	// test valid request where metric already exists
	createMetric("test8", []string{"id", "type"})

	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&createMetricRequest{
		Metric: "test8",
		Tags:   []string{"id", "type"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest("POST", "/create_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	createMetricHandler(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 409 {
		t.Fatal()
	}

	// test valid request
	deleteMetric("test4")

	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&createMetricRequest{
		Metric: "test4",
		Tags:   []string{"id", "type"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest("POST", "/create_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	createMetricHandler(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}
}

func TestDeleteMetricHandler(t *testing.T) {
	// test without content-type set
	createMetric("test5", []string{})

	body := &bytes.Buffer{}
	err := json.NewEncoder(body).Encode(&deleteMetricRequest{
		Metric: "test5",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("DELETE", "/delete_metric", body)
	w := httptest.NewRecorder()

	deleteMetricHandler(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}

	serverError := &serverError{}
	json.NewDecoder(resp.Body).Decode(serverError)
	if serverError.Error != "content-type not set" {
		t.Fatal()
	}

	// test valid request
	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&deleteMetricRequest{
		Metric: "test5",
	})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("DELETE", "/delete_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	deleteMetricHandler(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	// test request with nonexistent metric
	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&deleteMetricRequest{
		Metric: "test999z",
	})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("DELETE", "/delete_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	deleteMetricHandler(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Fatal()
	}
}

func TestInsertPointsHandler(t *testing.T) {
	// test invalid query
	req := httptest.NewRequest("POST", "/insert_points", nil)
	w := httptest.NewRecorder()
	insertPointsHandler(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}
	// test valid query
	deleteMetric("test6")
	createMetric("test6", []string{"id", "type"})

	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	body := &bytes.Buffer{}
	body.WriteString(fmt.Sprintf("test6,id=28084 type=high,18765003.4 %d\n", baseTime.UnixNano()))
	body.WriteString(fmt.Sprintf("test6,id=28084 type=high,18581431.53 %d\n", baseTime.Add(time.Minute).UnixNano()))
	body.WriteString(fmt.Sprintf("test6,id=28084 type=high,18532847.21 %d\n", baseTime.Add(time.Minute*2).UnixNano()))

	req = httptest.NewRequest("POST", "/insert_points", body)
	req.Header.Add("Content-Type", "application/x.simpletsdb.points")

	w = httptest.NewRecorder()

	insertPointsHandler(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	pts, err := queryPoints(&pointsQuery{
		Metric: "test6",
		Start:  baseTime.UnixNano(),
		Tags: map[string]string{
			"id": "28084",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []*point{
		{Value: 18765003.4, Timestamp: 946684800000000000},
		{Value: 18581431.53, Timestamp: 946684860000000000},
		{Value: 18532847.21, Timestamp: 946684920000000000},
	}, pts)
}

func TestQueryPointsHandler(t *testing.T) {
	// insert points first
	deleteMetric("test6")
	createMetric("test6", []string{"id", "type"})
	queries := []*insertPointQuery{}
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	vals := []float64{18765003.4, 18581431.53, 18631954.11}
	for i := 0; i < 3; i++ {
		pt, _ := parseLine([]byte(fmt.Sprintf("test6,id=28084 type=high,%f %d\n", vals[i], baseTime.Add(time.Minute*time.Duration(i)).UnixNano())))
		queries = append(queries, pt)
	}
	err := insertPoints(queries)
	if err != nil {
		t.Fatal(err)
	}

	buf := &bytes.Buffer{}
	err = json.NewEncoder(buf).Encode(&pointsQuery{
		Metric: "test6",
		Start:  baseTime.UnixNano(),
		End:    baseTime.Add(time.Minute * 3).UnixNano(),
		Tags: map[string]string{
			"id":   "28084",
			"type": "high",
		},
		Window: map[string]interface{}{
			"every":       "1m",
			"createEmpty": true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/query_points", buf)
	req.Header.Add("Content-Type", "application/json")

	w := httptest.NewRecorder()

	queryPointsHandler(w, req, nil)

	resp := w.Result()

	var respPoints []*point
	err = json.NewDecoder(resp.Body).Decode(&respPoints)

	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []*point{
		{Value: 18765003.4, Timestamp: 946684800000000000},
		{Value: 18581431.53, Timestamp: 946684860000000000},
		{Value: 18631954.11, Timestamp: 946684920000000000},
		{Null: true, Timestamp: 946684980000000000},
	}, respPoints)
}

func TestDeletePointsHandler(t *testing.T) {
	deleteMetric("test10")
	createMetric("test10", []string{"id"})
	queries := []*insertPointQuery{}
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	for i := 0; i < 5; i++ {
		q, _ := parseLine([]byte(fmt.Sprintf("test10,id=28084,999 %d\n", baseTime.Add(time.Minute*time.Duration(i)).UnixNano())))
		queries = append(queries, q)
	}
	if err := insertPoints(queries); err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(&deletePointsQuery{
		Metric: "test10",
		Start:  baseTime.Add(time.Minute).UnixNano(),
		End:    baseTime.Add(time.Minute * 3).UnixNano(),
		Tags: map[string]string{
			"id": "28084",
		},
	}); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/delete_points", buf)
	req.Header.Add("Content-Type", "application/json")

	w := httptest.NewRecorder()

	deletePointsHandler(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	points, err := queryPoints(&pointsQuery{
		Metric: "test10",
		Start:  baseTime.UnixNano(),
		End:    baseTime.Add(time.Minute * 4).UnixNano(),
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []*point{
		{Value: 999, Timestamp: baseTime.UnixNano()},
		{Value: 999, Timestamp: baseTime.Add(time.Minute * 4).UnixNano()},
	}, points)
}
