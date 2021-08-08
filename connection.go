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
	metricsTable      = `simpletsdb_metrics`
	downsamplersTable = `simpletsdb_downsamplers`
	downsamplers      []*downsampler
)

func initDB(pgUser, pgPassword, pgHost string, pgPort int, pgDB, pgSSLMode string, nWorkers int) (*dbConn, chan struct{}) {
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
  query jsonb
)
		`, downsamplersTable))

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

	cancelDownsampleWait := make(chan struct{})
	go handleDownsamplers(db, cancelDownsampleWait)

	return db, cancelDownsampleWait
}
