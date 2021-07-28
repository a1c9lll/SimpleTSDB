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

func initServer(tsdbHost string, tsdbPort int, tsdbReadTimeout, tsdbWriteTimeout time.Duration, readLineProtocolBufferSizeP int) {
	router := httprouter.New()
	router.POST("/insert_points", insertPointsHandler)
	router.POST("/query_points", queryPointsHandler)
	router.DELETE("/delete_points", deletePointsHandler)

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
	return json.NewEncoder(w).Encode(&serverError{
		Error: err,
	})
}

/*
Returns 400 on invalid request
Returns 200 on successful insertion
Returns 404 if metric doesn't exist
*/
func insertPointsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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
	queries := []*insertPointQuery{}
	for scanner.Scan() {
		query, err := parseLine(scanner.Bytes())
		if err != nil {
			log.Error(err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Error(err0)
			}
			return
		}
		queries = append(queries, query)
	}

	err := insertPoints(queries)
	if err != nil {
		if err == errMetricDoesNotExist {
			log.Println(err)
			w.WriteHeader(404)
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
Returns 200 on successful query
Returns 404 if metric doesn't exist
*/
func queryPointsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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

	req := &pointsQuery{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	pts, err := queryPoints(req)

	if err != nil {
		if err.Error() == "metric does not exist" {
			log.Error(err)
			w.WriteHeader(404)
		} else {
			log.Error(err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Error(err0)
			}
		}
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(points(pts)); err != nil {
		log.Error(err)
	}
}

/*
Returns 400 on invalid request
Returns 404 on metrics that don't exist
Returns 200 on successful deletion
*/
func deletePointsHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
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

	req := &deletePointsQuery{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	if err := deletePoints(req); err != nil {
		log.Error(err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Error(err0)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}
