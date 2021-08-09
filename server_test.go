package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInsertPointsHandler(t *testing.T) {
	// test invalid query
	req := httptest.NewRequest("POST", "/insert_points", nil)
	w := httptest.NewRecorder()
	insertPointsHandler(db0, w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 400 {
		t.Fatal()
	}
	// test valid query
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	body := &bytes.Buffer{}
	body.WriteString(fmt.Sprintf("test7,id=28084 type=high,18765003.4 %d\n", baseTime.UnixNano()))
	body.WriteString(fmt.Sprintf("test7,id=28084 type=high,18581431.53 %d\n", baseTime.Add(time.Minute).UnixNano()))
	body.WriteString(fmt.Sprintf("test7,id=28084 type=high,18532847.21 %d\n", baseTime.Add(time.Minute*2).UnixNano()))

	req = httptest.NewRequest("POST", "/insert_points", body)
	req.Header.Add("Content-Type", "application/x.simpletsdb.points")

	w = httptest.NewRecorder()

	insertPointsHandler(db0, w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	pts, err := queryPoints(db0, priorityCRUD, &pointsQuery{
		Metric: "test7",
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
	queries := []*insertPointQuery{}
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	vals := []float64{18765003.4, 18581431.53, 18631954.11}
	for i := 0; i < 3; i++ {
		pt, _ := parseLineProtocol([]byte(fmt.Sprintf("test6,id=28084 type=high,%f %d\n", vals[i], baseTime.Add(time.Minute*time.Duration(i)).UnixNano())))
		queries = append(queries, pt)
	}
	err := insertPoints(db0, queries)
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

	queryPointsHandler(db0, w, req, nil)

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
	queries := []*insertPointQuery{}
	baseTime := mustParseTime("2000-01-01T00:00:00Z")
	for i := 0; i < 5; i++ {
		q, _ := parseLineProtocol([]byte(fmt.Sprintf("test10,id=28084,999 %d\n", baseTime.Add(time.Minute*time.Duration(i)).UnixNano())))
		queries = append(queries, q)
	}
	if err := insertPoints(db0, queries); err != nil {
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

	deletePointsHandler(db0, w, req, nil)

	resp := w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	points, err := queryPoints(db0, priorityCRUD, &pointsQuery{
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

func TestDownsamplerAPI(t *testing.T) {
	ds := &downsampler{
		Metric:    "test123",
		OutMetric: "test123_30m",
		RunEvery:  "30m",
		Query: &downsampleQuery{
			Tags: map[string]string{
				"id": "2",
			},
			Window: map[string]interface{}{
				"every": "30m",
			},
			Aggregators: []*aggregatorQuery{
				{Name: "mean"},
			},
		},
	}

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(ds); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/add_downsampler", buf)
	req.Header.Add("Content-Type", "application/json")

	w := httptest.NewRecorder()

	addDownsamplerHandler(db0, downsamplersCountTest, downsamplersCancelWaitTest, w, req, nil)

	resp := w.Result()

	bod, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("got status code: %d, body: %s", resp.StatusCode, string(bod))
	}

	// test list
	req = httptest.NewRequest("GET", "/list_downsamplers", nil)

	w = httptest.NewRecorder()

	listDownsamplersHandler(db0, w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	downsamplers0 := []*downsampler{}
	if err = json.NewDecoder(resp.Body).Decode(&downsamplers0); err != nil {
		t.Fatal(err)
	}

	if len(downsamplers0) == 0 {
		t.Fatal("expected > 0 downsamplers")
	}

	// test delete
	for _, d := range downsamplers0 {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(&deleteDownsamplerRequest{
			ID: d.ID,
		}); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("POST", "/delete_downsampler", buf)
		req.Header.Add("Content-Type", "application/json")

		w := httptest.NewRecorder()

		deleteDownsamplerHandler(db0, w, req, nil)

		resp := w.Result()

		body, _ := ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatal(string(body))
		}
	}

	ds0, err := selectDownsamplers(db0)
	if err != nil {
		t.Fatal(err)
	}

	if len(ds0) != 0 {
		t.Fatalf("expected 0 downsamplers, got %d", len(ds0))
	}

	// test add downsamplers
	dss := []*downsampler{
		{
			Metric:    "test123",
			OutMetric: "test123_30m",
			RunEvery:  "30m",
			Query: &downsampleQuery{
				Tags: map[string]string{
					"id": "2",
				},
				Window: map[string]interface{}{
					"every": "30m",
				},
				Aggregators: []*aggregatorQuery{
					{Name: "mean"},
				},
			},
		}, {
			Metric:    "test123",
			OutMetric: "test123_30m",
			RunEvery:  "30m",
			Query: &downsampleQuery{
				Tags: map[string]string{
					"id": "3",
				},
				Window: map[string]interface{}{
					"every": "30m",
				},
				Aggregators: []*aggregatorQuery{
					{Name: "mean"},
				},
			},
		},
	}

	buf = &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(dss); err != nil {
		t.Fatal(err)
	}

	req = httptest.NewRequest("POST", "/add_downsamplers", buf)
	req.Header.Add("Content-Type", "application/json")

	w = httptest.NewRecorder()

	addDownsamplersHandler(db0, downsamplersCountTest, downsamplersCancelWaitTest, w, req, nil)

	resp = w.Result()

	bod, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("got status code: %d, body: %s", resp.StatusCode, string(bod))
	}

	// list after add downsamplers

	req = httptest.NewRequest("GET", "/list_downsamplers", nil)

	w = httptest.NewRecorder()

	listDownsamplersHandler(db0, w, req, nil)

	resp = w.Result()

	if resp.StatusCode != 200 {
		t.Fatal()
	}

	downsamplers1 := []*downsampler{}
	if err = json.NewDecoder(resp.Body).Decode(&downsamplers1); err != nil {
		t.Fatal(err)
	}

	if len(downsamplers1) != 2 {
		t.Fatalf("expected 2 downsamplers, got %d", len(downsamplers1))
	}

	// clean up
	for _, d := range downsamplers1 {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(&deleteDownsamplerRequest{
			ID: d.ID,
		}); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest("POST", "/delete_downsampler", buf)
		req.Header.Add("Content-Type", "application/json")

		w := httptest.NewRecorder()

		deleteDownsamplerHandler(db0, w, req, nil)

		resp := w.Result()

		body, _ := ioutil.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatal(string(body))
		}
	}
}
