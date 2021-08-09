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

func initServer(db *dbConn, downsamplersCountChan chan int, cancelDownsampleWait []chan struct{}, tsdbHost string, tsdbPort int, tsdbReadTimeout, tsdbWriteTimeout time.Duration, readLineProtocolBufferSizeP int) {
	router := httprouter.New()
	router.POST("/insert_points", withDB(db, insertPointsHandler))
	router.POST("/query_points", withDB(db, queryPointsHandler))
	router.DELETE("/delete_points", withDB(db, deletePointsHandler))
	router.POST("/add_downsampler", withDbAndDownsamplerChannels(db, downsamplersCountChan, cancelDownsampleWait, addDownsamplerHandler))
	router.GET("/list_downsamplers", withDB(db, listDownsamplersHandler))
	router.DELETE("/delete_downsampler", withDB(db, deleteDownsamplerHandler))

	s := &http.Server{
		Addr:           fmt.Sprintf("%s:%d", tsdbHost, tsdbPort),
		Handler:        router,
		ReadTimeout:    tsdbReadTimeout,
		WriteTimeout:   tsdbWriteTimeout,
		MaxHeaderBytes: 1 << 20,
	}

	readLineProtocolBufferSize = readLineProtocolBufferSizeP

	log.Fatalf("initServer: %s", s.ListenAndServe())
}

func withDbAndDownsamplerChannels(db *dbConn, downsamplersCountChan chan int, cancelDownsampleWait []chan struct{}, fn func(*dbConn, chan int, []chan struct{}, http.ResponseWriter, *http.Request, httprouter.Params)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		fn(db, downsamplersCountChan, cancelDownsampleWait, w, r, ps)
	}
}

func withDB(db *dbConn, fn func(*dbConn, http.ResponseWriter, *http.Request, httprouter.Params)) func(http.ResponseWriter, *http.Request, httprouter.Params) {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		fn(db, w, r, ps)
	}
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
func insertPointsHandler(db *dbConn, w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("insert_points request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("insert_points: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Errorf("insertPointsHandler: %s", err0)
		}
		return
	}

	if typeHeader[0] != "application/x.simpletsdb.points" {
		log.Error("insert_points: content-type must be application/x.simpletsdb.points")
		if err0 := write400Error(w, "content-type must be application/x.simpletsdb.points"); err0 != nil {
			log.Errorf("insertPointsHandler: %s", err0)
		}
		return
	}

	defer r.Body.Close()

	scanner := bufio.NewScanner(r.Body)
	buf := make([]byte, readLineProtocolBufferSize)
	scanner.Buffer(buf, readLineProtocolBufferSize)
	queries := []*insertPointQuery{}
	for scanner.Scan() {
		query, err := parseLineProtocol(scanner.Bytes())
		if err != nil {
			log.Errorf("insertPointsHandler: %s", err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Errorf("insertPointsHandler: %s", err0)
			}
			return
		}
		queries = append(queries, query)
	}

	err := insertPoints(db, queries)
	if err != nil {
		if err == errMetricDoesNotExist {
			w.WriteHeader(404)
		} else {
			log.Errorf("insertPointsHandler: %s", err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Errorf("insertPointsHandler: %s", err0)
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
func queryPointsHandler(db *dbConn, w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("query_points request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("query_points: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Errorf("queryPointsHandler: %s", err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("query_points: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Errorf("queryPointsHandler: %s", err0)
		}
		return
	}

	defer r.Body.Close()

	req := &pointsQuery{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Errorf("queryPointsHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("queryPointsHandler: %s", err0)
		}
		return
	}

	pts, err := queryPoints(db, priorityCRUD, req)

	if err != nil {
		if err.Error() == "metric does not exist" {
			log.Errorf("queryPointsHandler: %s", err)
			w.WriteHeader(404)
		} else {
			log.Errorf("queryPointsHandler: %s", err)
			if err0 := write400Error(w, err.Error()); err0 != nil {
				log.Errorf("queryPointsHandler: %s", err0)
			}
		}
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(points(pts)); err != nil {
		log.Errorf("queryPointsHandler: %s", err)
	}
}

/*
Returns 400 on invalid request
Returns 404 on metrics that don't exist
Returns 200 on successful deletion
*/
func deletePointsHandler(db *dbConn, w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("delete_points request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("delete_points: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Errorf("deletePointsHandler: %s", err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("delete_points: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Errorf("deletePointsHandler: %s", err0)
		}
		return
	}

	defer r.Body.Close()

	req := &deletePointsQuery{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Errorf("deletePointsHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("deletePointsHandler: %s", err0)
		}
		return
	}

	if err := deletePoints(db, req); err != nil {
		log.Errorf("deletePointsHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("deletePointsHandler: %s", err0)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

/*
Returns 400 on invalid request
Returns 200 on successful request
*/
func addDownsamplerHandler(db *dbConn, downsamplersCountChan chan int, cancelDownsampleWait []chan struct{}, w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("add_downsampler request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("add_downsampler: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Errorf("addDownsamplerHandler: %s", err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("add_downsampler: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Errorf("addDownsamplerHandler: %s", err0)
		}
		return
	}

	defer r.Body.Close()

	req := &downsampler{}

	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		log.Errorf("addDownsamplerHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("addDownsamplerHandler: %s", err0)
		}
		return
	}

	err := addDownsampler(db, downsamplersCountChan, cancelDownsampleWait, req)

	if err != nil {
		log.Errorf("addDownsamplerHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("addDownsamplerHandler: %s", err0)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}

/*
Returns 200 on successful request
Returns 500 on server failure
*/
func listDownsamplersHandler(db *dbConn, w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("list_downsampler request from %s", r.RemoteAddr)

	downsamplers, err := selectDownsamplers(db)
	if err != nil {
		log.Errorf("listDownsamplersHandler: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	err = json.NewEncoder(w).Encode(downsamplers)
	if err != nil {
		log.Errorf("listDownsamplersHandler: %s", err)
	}
}

/*
Returns 400 on invalid request
Returns 200 on successful request
*/
func deleteDownsamplerHandler(db *dbConn, w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Infof("delete_downsampler request from %s", r.RemoteAddr)

	typeHeader := r.Header.Values("Content-Type")

	if len(typeHeader) != 1 {
		log.Error("delete_downsampler: content-type not set")
		if err0 := write400Error(w, "content-type not set"); err0 != nil {
			log.Errorf("deleteDownsamplerHandler: %s", err0)
		}
		return
	}

	if typeHeader[0] != "application/json" {
		log.Error("delete_downsampler: content-type must be application/json")
		if err0 := write400Error(w, "content-type must be application/json"); err0 != nil {
			log.Errorf("deleteDownsamplerHandler: %s", err0)
		}
		return
	}

	defer r.Body.Close()

	del := &deleteDownsamplerRequest{}
	if err := json.NewDecoder(r.Body).Decode(del); err != nil {
		log.Errorf("deleteDownsamplerHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("deleteDownsamplerHandler: %s", err0)
		}
		return
	}

	if err := deleteDownsampler(db, del); err != nil {
		log.Errorf("deleteDownsamplerHandler: %s", err)
		if err0 := write400Error(w, err.Error()); err0 != nil {
			log.Errorf("deleteDownsamplerHandler: %s", err0)
		}
		return
	}

	w.WriteHeader(http.StatusOK)
}
