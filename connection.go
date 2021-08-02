package main

import (
	"container/heap"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	_ "github.com/lib/pq"
)

var (
	metricsTable                 = `simpletsdb_metrics`
	downsamplersTable            = `simpletsdb_downsamplers`
	downsamplers                 []*downsampler
	errLastDownsampledWindowType = errors.New("incorrect type for lastDownsampledWindow")
)

func initDB(pgUser, pgPassword, pgHost string, pgPort int, pgDB, pgSSLMode string, nWorkers int) *dbConn {
	var passwordString string
	if pgPassword != "" {
		passwordString = fmt.Sprintf("password='%s' ", pgPassword)
	}
	connStr0 := fmt.Sprintf("user=%s %shost='%s' port=%d sslmode=%s", pgUser, passwordString, pgHost, pgPort, pgSSLMode)
	var err error
	session, err := sql.Open("postgres", connStr0)
	if err != nil {
		log.Fatal(err)
	}
	if err := session.Ping(); err != nil {
		log.Fatal(err)
	}
	session.Exec(fmt.Sprintf("create database %s", pgDB))
	if ok, err := databaseExists(pgDB, session); err != nil {
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("could not create database %s", pgDB)
	}
	session.Close()

	connStr := fmt.Sprintf("user=%s %shost='%s' port=%d dbname=%s sslmode=%s", pgUser, passwordString, pgHost, pgPort, pgDB, pgSSLMode)
	session, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	if err := session.Ping(); err != nil {
		log.Fatal(err)
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
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("could not create %s table", metricsTable)
	}

	if ok, err := tableExists(db, downsamplersTable); err != nil {
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("could not create %s table", downsamplersTable)
	}

	go handleDownsamplers(db)

	return db
}

//select id, out_metric, last_updated, run_every,  (last_updated + run_every)::bigint - (extract(epoch from now())*1000000000)::bigint as time_until_update from simpletsdb_downsamplers;
func handleDownsamplers(db *dbConn) {
	for {
		var (
			timeUntilUpdate int64
			ds              = &downsampler{}
		)
		err := db.Query(priorityDownsamplers, func(db *sql.DB) error {
			var (
				queryJSON             string
				runEvery              int64
				lastDownsampledWindow interface{}
			)
			vals := []interface{}{
				time.Now().UnixNano(),
			}
			row := db.QueryRow("select id,metric,out_metric,run_every,last_downsampled_window,query,(last_updated + run_every)::bigint - $1 as time_until_update from simpletsdb_downsamplers order by time_until_update asc limit 1", vals...)
			err := row.Scan(
				&ds.ID,
				&ds.Metric,
				&ds.OutMetric,
				&runEvery,
				&lastDownsampledWindow,
				&queryJSON,
				&timeUntilUpdate,
			)
			if err != nil {
				return err
			}
			if lastDownsampledWindow != nil {
				switch v := lastDownsampledWindow.(type) {
				case int:
					ds.LastDownsampledWindow = int64(v)
				case int32:
					ds.LastDownsampledWindow = int64(v)
				case int64:
					ds.LastDownsampledWindow = v
				default:
					return errLastDownsampledWindowType
				}
			}
			ds.RunEvery = time.Duration(runEvery).String()
			ds.RunEveryDur = time.Duration(runEvery)

			ds.Query = &downsampleQuery{}
			err = json.Unmarshal([]byte(queryJSON), &ds.Query)
			if err != nil {
				return err
			}
			if err := row.Err(); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if err.Error() == "sql: no rows in result set" {
				time.Sleep(time.Second)
				continue
			}
			panic(err)
		}
		if timeUntilUpdate > 0 {
			log.Infof("waiting %s for downsampler %d", time.Duration(timeUntilUpdate), ds.ID)
			time.Sleep(time.Duration(timeUntilUpdate))
		}
		t0 := time.Now()
		err = downsample(db, ds)
		if err != nil {
			panic(err)
		}

		err = db.Query(priorityDownsamplers, func(db *sql.DB) error {
			query := fmt.Sprintf("UPDATE %s SET last_updated = $1 WHERE id = $2", downsamplersTable)
			vals := []interface{}{
				time.Now().UnixNano(),
				ds.ID,
			}
			_, err := db.Exec(query, vals...)
			return err
		})
		if err != nil {
			panic(err)
		}
		t1 := time.Since(t0)
		log.Debugf("downsample %d took %dms", ds.ID, t1.Milliseconds())
	}
}
