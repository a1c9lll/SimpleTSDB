package main

import (
	"container/heap"
	"database/sql"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	_ "github.com/lib/pq"
)

var (
	metricsTable           = `simpletsdb_metrics`
	downsamplersTable      = `simpletsdb_downsamplers`
	metaTable              = `simpletsdb_meta`
	downsamplers           []*downsampler
	downsamplerWorkerCount = 32
)

func initDB(pgUser, pgPassword, pgHost string, pgPort int, pgDB, pgSSLMode string, nWorkers int) (*dbConn, chan int, []chan struct{}) {
	var passwordString string
	if pgPassword != "" {
		passwordString = fmt.Sprintf("password='%s' ", pgPassword)
	}
	connStr0 := fmt.Sprintf("user=%s %shost='%s' port=%d sslmode=%s", pgUser, passwordString, pgHost, pgPort, pgSSLMode)
	var err error
	session, err := sql.Open("postgres", connStr0)
	if err != nil {
		log.Fatalf("initDB: %s", err)
	}
	if err := session.Ping(); err != nil {
		log.Fatalf("initDB: %s", err)
	}
	session.Exec(fmt.Sprintf("create database %s", pgDB))
	if ok, err := databaseExists(pgDB, session); err != nil {
		log.Fatalf("initDB: %s", err)
	} else if !ok {
		log.Fatalf("initDB: could not create database %s", pgDB)
	}
	session.Close()

	connStr := fmt.Sprintf("user=%s %shost='%s' port=%d dbname=%s sslmode=%s", pgUser, passwordString, pgHost, pgPort, pgDB, pgSSLMode)
	session, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("initDB: %s", err)
	}
	if err := session.Ping(); err != nil {
		log.Fatalf("initDB: %s", err)
	}

	session.Exec(fmt.Sprintf(`
CREATE TABLE %s (
	metric text,
	value double precision,
	timestamp bigint,
	tags jsonb,
	UNIQUE(metric, timestamp, tags)
)
	`, metricsTable))

	session.Exec(fmt.Sprintf(`
CREATE TABLE %s (
  id serial,
  metric text,
  out_metric text,
  run_every bigint,
  last_downsampled_window bigint,
	last_updated bigint NOT NULL,
	worker_id int,
  query jsonb
)
		`, downsamplersTable))

	session.Exec(fmt.Sprintf(`
		CREATE TABLE %s (
			worker_id_count int
		)
				`, metaTable))

	db := &dbConn{queue: &priorityQueue{}, cond: sync.NewCond(&sync.Mutex{})}
	heap.Init(db.queue)

	for i := 0; i < nWorkers; i++ {
		go func() {
			for {
				db.cond.L.Lock()
				for db.queue.Len() == 0 {
					db.cond.Wait()
				}
				item := heap.Pop(db.queue).(*item)
				db.cond.L.Unlock()

				err := item.fn(session)
				item.done <- err
			}
		}()
	}

	if ok, err := tableExists(db, metricsTable); err != nil {
		log.Fatalf("initDB: %s", err)
	} else if !ok {
		log.Fatalf("initDB: could not create %s table", metricsTable)
	}

	if ok, err := tableExists(db, downsamplersTable); err != nil {
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("initDB: could not create %s table", downsamplersTable)
	}

	if ok, err := tableExists(db, metaTable); err != nil {
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("initDB: could not create %s table", metaTable)
	}

	downsamplersCount, err := selectDownsamplersCount(db)
	if err != nil && err.Error() == errStrNoRowsInResultSet {
		if err0 := insertDownsamplersInitialCount(db); err0 != nil {
			log.Fatalf("initDB: downsampleCoordinator start: %s", err0)
		}
	} else if err != nil {
		log.Fatalf("initDB: downsampleCoordinator start: %s", err)
	}

	nextDownsamplerID := make(chan int)
	go downsampleCountCoordinator(db, downsamplersCount, nextDownsamplerID)

	cancelDownsampleWait := make([]chan struct{}, downsamplerWorkerCount)
	for i := 0; i < downsamplerWorkerCount; i++ {
		i := i
		cancelDownsampleWait[i] = make(chan struct{})
		go handleDownsamplers(db, i, cancelDownsampleWait[i])
	}

	return db, nextDownsamplerID, cancelDownsampleWait
}
