package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"simpletsdb/core"
	"simpletsdb/datastore"
	"simpletsdb/util"
	"time"

	"github.com/julienschmidt/httprouter"
)

var (
	readLineProtocolBufferSize = 65536
)

func Init(tsdbHost string, tsdbPort int, tsdbReadTimeout, tsdbWriteTimeout time.Duration, readLineProtocolBufferSizeP int) {
	router := httprouter.New()
	router.GET("/metric_exists", MetricExists)
	router.POST("/create_metric", MetricExists)
	router.DELETE("/delete_metric", DeleteMetric)
	router.POST("/insert_points", InsertPoints)

	s := &http.Server{
		Addr:           fmt.Sprintf("%s:%d", tsdbHost, tsdbPort),
		Handler:        router,
		ReadTimeout:    tsdbReadTimeout,
		WriteTimeout:   tsdbWriteTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	readLineProtocolBufferSize = readLineProtocolBufferSizeP

	log.Fatal(s.ListenAndServe())
}

func write400Error(w http.ResponseWriter, err string) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	return json.NewEncoder(w).Encode(&ServerError{
		Error: err,
	})
}

/*
Returns 400 on invalid request
Returns 200 on successful request
Returns 500 on server failure
*/
func MetricExists(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if err := r.ParseForm(); err != nil {
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Println(err0)
		}
		return
	}
	var (
		metric string
	)
	if metricForm, ok := r.Form["metric"]; !ok {
		if err := write400Error(w, "metric is required"); err != nil {
			log.Println(err)
		}
		return
	} else {
		if len(metricForm) != 1 {
			if err := write400Error(w, "only one metric allowed"); err != nil {
				log.Println(err)
			}
			return
		}
		metric = metricForm[0]
	}

	if exists, err := datastore.MetricExists(metric); err != nil {
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Println(err0)
		}
		return
	} else {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&MetricExistsResponse{
			Exists: exists,
		}); err != nil {
			log.Println(err)
			return
		}
	}
}

/*
Returns 400 on invalid request
Returns 409 on metrics that already exist
Returns 200 on successful creation
*/
func CreateMetric(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Println(err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Println(err0)
		}
		return
	}

	defer r.Body.Close()

	req := &CreateMetricRequest{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Println(err0)
		}
		return
	}

	err := datastore.CreateMetric(req.Metric, req.Tags)
	if err != nil {
		if err.Error() == "metric already exists" {
			w.WriteHeader(http.StatusConflict)
		} else {
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Println(err0)
			}
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

/*
Returns 400 on invalid request
Returns 404 on metrics that don't exist
Returns 200 on successful deletion
*/
func DeleteMetric(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Println(err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Println(err0)
		}
		return
	}

	defer r.Body.Close()

	req := &DeleteMetricRequest{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Println(err0)
		}
		return
	}

	err := datastore.DeleteMetric(req.Metric)
	if err != nil {
		if err.Error() == "metric does not exist" {
			w.WriteHeader(http.StatusNotFound)
		} else {
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Println(err0)
			}
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

func InsertPoints(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Println(err0)
		}
		return
	}

	if typeHeader[0] != "text/plain" {
		if err0 := write400Error(w, "content-type must be text/plain"); err0 != nil {
			log.Println(err0)
		}
		return
	}

	defer r.Body.Close()

	queries := []*core.InsertPointQuery{}

	scanner := bufio.NewScanner(r.Body)
	buf := make([]byte, readLineProtocolBufferSize)
	scanner.Buffer(buf, readLineProtocolBufferSize)

	for scanner.Scan() {
		query, err := util.ParseLine(scanner.Bytes())
		if err != nil {
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Println(err0)
			}
			log.Println(err)
			return
		}
		queries = append(queries, query)
	}

	err := datastore.InsertPoints(queries)
	if err != nil {
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Println(err0)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}
