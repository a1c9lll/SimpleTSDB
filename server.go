package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/julienschmidt/httprouter"
)

var (
	readLineProtocolBufferSize = 65536
)

func InitServer(tsdbHost string, tsdbPort int, tsdbReadTimeout, tsdbWriteTimeout time.Duration, readLineProtocolBufferSizeP int) {
	router := httprouter.New()
	router.GET("/metric_exists", MetricExistsHandler)
	router.POST("/create_metric", CreateMetricHandler)
	router.DELETE("/delete_metric", DeleteMetricHandler)
	router.POST("/insert_points", InsertPointsHandler)
	router.POST("/query_points", QueryPointsHandler)
	router.DELETE("/delete_points", DeletePointsHandler)

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
func MetricExistsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("metric_exists request from %s", r.RemoteAddr)
	if err := r.ParseForm(); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}
	var (
		metric string
	)
	if metricForm, ok := r.Form["metric"]; !ok {
		log.Error("metric_exists: metric is required")
		if err := write400Error(w, "metric is required"); err != nil {
			log.Error(err)
		}
		return
	} else {
		if len(metricForm) != 1 {
			log.Error("metric_exists: only one metric allowed")
			if err := write400Error(w, "only one metric allowed"); err != nil {
				log.Error(err)
			}
			return
		}
		metric = metricForm[0]
	}

	if exists, err := MetricExists(metric); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	} else {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&MetricExistsResponse{
			Exists: exists,
		}); err != nil {
			log.Error(err)
			return
		}
	}
}

/*
Returns 400 on invalid request
Returns 409 on metrics that already exist
Returns 200 on successful creation
*/
func CreateMetricHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("create_metric request from %s", r.RemoteAddr)
	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("create_metric: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("create_metric: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	defer r.Body.Close()

	req := &CreateMetricRequest{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	err := CreateMetric(req.Metric, req.Tags)
	if err != nil {
		if err.Error() == "metric already exists" {
			log.Error("create_metric: metric already exists")
			w.WriteHeader(http.StatusConflict)
		} else {
			log.Error(err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Error(err0)
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
func DeleteMetricHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("delete_metric request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("delete_metric: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("delete_metric: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	defer r.Body.Close()

	req := &DeleteMetricRequest{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	err := DeleteMetric(req.Metric)
	if err != nil {
		if err.Error() == "metric does not exist" {
			log.Error("delete_metric: metric does not exist")
			w.WriteHeader(http.StatusNotFound)
		} else {
			log.Error(err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Error(err0)
			}
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

/*
Returns 400 on invalid request
Returns 200 on successful insertion
*/
func InsertPointsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("insert_points request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("insert_points: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if typeHeader[0] != "application/x.simpletsdb.points" {
		log.Error("insert_points: content-type must be application/x.simpletsdb.points")
		if err0 := write400Error(w, "content-type must be application/x.simpletsdb.points"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	defer r.Body.Close()

	scanner := bufio.NewScanner(r.Body)
	buf := make([]byte, readLineProtocolBufferSize)
	scanner.Buffer(buf, readLineProtocolBufferSize)

	for scanner.Scan() {
		query, err := ParseLine(scanner.Bytes())
		if err != nil {
			log.Error(err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Error(err0)
			}
			return
		}

		err = InsertPoint(query)
		if err != nil {
			log.Error(err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Error(err0)
			}
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

/*
Returns 400 on invalid request
Returns 200 on successful query
*/
func QueryPointsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("query_points request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("query_points: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("query_points: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	defer r.Body.Close()

	req := &PointsQuery{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	points, err := QueryPoints(req)

	if err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(points); err != nil {
		log.Error(err)
	}
}

/*
Returns 400 on invalid request
Returns 404 on metrics that don't exist
Returns 200 on successful deletion
*/
func DeletePointsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("delete_points request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("delete_points: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("delete_points: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Error(err0)
		}
		return
	}

	defer r.Body.Close()

	req := &DeletePointsQuery{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if err := DeletePoints(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}
