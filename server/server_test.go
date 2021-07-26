package server

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"simpletsdb/datastore"
	"simpletsdb/util"
	"strconv"
	"testing"

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
	req := httptest.NewRequest("PUT", "/create_metric", body)
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

	req = httptest.NewRequest("PUT", "/create_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	CreateMetric(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 204 {
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

	req = httptest.NewRequest("PUT", "/create_metric", body)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	CreateMetric(w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 201 {
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
