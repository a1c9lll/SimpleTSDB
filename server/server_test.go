package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"simpletsdb/core"
	"simpletsdb/datastore"
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
	datastore.InitDB(cfg["postgres_username"], cfg["postgres_password"], cfg["postgres_host"], port, cfg["postgres_ssl_mode"])

	datastore.DeleteMetric("test0")

	err = datastore.CreateMetric("test0", []string{"id", "type"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMetricExists(t *testing.T) {
	// test invalid request
	req := httptest.NewRequest("GET", "/metric_exists", nil)
	w := httptest.NewRecorder()

	MetricExists(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal()
	}

	resp0 := &ServerError{}
	if err := json.NewDecoder(resp.Body).Decode(resp0); err != nil {
		t.Fatal(err)
	}

	if resp0.Error != "metric is required" {
		t.Fatal()
	}

	// test valid request and exists=true
	req = httptest.NewRequest("GET", "/metric_exists?metric=test0", nil)
	w = httptest.NewRecorder()

	MetricExists(w, req, nil)

	resp = w.Result()

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal()
	}

	resp1 := &MetricExistsResponse{}
	if err := json.NewDecoder(resp.Body).Decode(resp1); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &MetricExistsResponse{
		Exists: true,
	}, resp1)

	// test valid request and exists=false
	req = httptest.NewRequest("GET", "/metric_exists?metric=test999x", nil)
	w = httptest.NewRecorder()

	MetricExists(w, req, nil)

	resp = w.Result()

	if resp.Header.Get("Content-Type") != "application/json" {
		t.Fatal()
	}

	resp2 := &MetricExistsResponse{}
	if err := json.NewDecoder(resp.Body).Decode(resp2); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, &MetricExistsResponse{
		Exists: false,
	}, resp2)
}

func TestCreateMetric(t *testing.T) {
	// test without content-type set
	body := &bytes.Buffer{}
	err := json.NewEncoder(body).Encode(&CreateMetricRequest{
		Metric: "test0",
		Tags:   []string{"id", "type"},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/create_metric", body)
	w := httptest.NewRecorder()

	CreateMetric(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}

	serverError := &ServerError{}
	json.NewDecoder(resp.Body).Decode(serverError)
	if serverError.Error != "content-type not set" {
		t.Fatal()
	}

	// test valid request where metric already exists
	datastore.CreateMetric("test0", []string{"id", "type"})

	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&CreateMetricRequest{
		Metric: "test0",
		Tags:   []string{"id", "type"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest("POST", "/create_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	CreateMetric(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 409 {
		t.Fatal()
	}

	// test valid request
	datastore.DeleteMetric("test4")

	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&CreateMetricRequest{
		Metric: "test4",
		Tags:   []string{"id", "type"},
	})
	if err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest("POST", "/create_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	CreateMetric(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}
}

func TestDeleteMetric(t *testing.T) {
	// test without content-type set
	datastore.CreateMetric("test5", []string{})

	body := &bytes.Buffer{}
	err := json.NewEncoder(body).Encode(&DeleteMetricRequest{
		Metric: "test5",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("DELETE", "/delete_metric", body)
	w := httptest.NewRecorder()

	DeleteMetric(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}

	serverError := &ServerError{}
	json.NewDecoder(resp.Body).Decode(serverError)
	if serverError.Error != "content-type not set" {
		t.Fatal()
	}

	// test valid request
	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&DeleteMetricRequest{
		Metric: "test5",
	})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("DELETE", "/delete_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	DeleteMetric(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	// test request with nonexistent metric
	body = &bytes.Buffer{}
	err = json.NewEncoder(body).Encode(&DeleteMetricRequest{
		Metric: "test999z",
	})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest("DELETE", "/delete_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	DeleteMetric(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 404 {
		t.Fatal()
	}
}

func TestInsertPoints(t *testing.T) {
	// test invalid query
	req := httptest.NewRequest("POST", "/insert_points", nil)
	w := httptest.NewRecorder()
	InsertPoints(w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}
	// test valid query
	datastore.DeleteMetric("test6")
	datastore.CreateMetric("test6", []string{"id", "type"})

	baseTime := util.MustParseTime("2000-01-01T00:00:00Z")
	body := &bytes.Buffer{}
	body.WriteString(fmt.Sprintf("test6,id=28084 type=high,18765003.4 %d\n", baseTime.UnixNano()))
	body.WriteString(fmt.Sprintf("test6,id=28084 type=high,18581431.53 %d\n", baseTime.Add(time.Minute).UnixNano()))
	body.WriteString(fmt.Sprintf("test6,id=28084 type=high,null %d\n", baseTime.Add(time.Minute*2).UnixNano()))

	req = httptest.NewRequest("POST", "/insert_points", body)
	req.Header.Add("Content-Type", "text/plain")

	w = httptest.NewRecorder()

	InsertPoints(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	pts, err := datastore.QueryPoints(&core.PointsQuery{
		Metric: "test6",
		Start:  baseTime.UnixNano(),
		Tags: map[string]string{
			"id": "28084",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []*core.Point{
		{Value: 18765003.4, Timestamp: 946684800000000000},
		{Value: 18581431.53, Timestamp: 946684860000000000},
		{Value: 0, Timestamp: 946684920000000000, Null: true},
	}, pts)
}
