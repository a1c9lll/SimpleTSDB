package main

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	_ "github.com/lib/pq"
)

var (
	session           *sql.DB
	metricsTable      = `simpletsdb_metrics`
	downsamplersTable = `simpletsdb_downsamplers`
	downsamplers      []*downsampler
)

func initDB(pgUser, pgPassword, pgHost string, pgPort int, pgDB, pgSSLMode string) {
	var passwordString string
	if pgPassword != "" {
		passwordString = fmt.Sprintf("password='%s' ", pgPassword)
	}
	connStr0 := fmt.Sprintf("user=%s %shost='%s' port=%d sslmode=%s", pgUser, passwordString, pgHost, pgPort, pgSSLMode)
	var err error
	session, err = sql.Open("postgres", connStr0)
	if err != nil {
		log.Fatal(err)
	}
	if err := session.Ping(); err != nil {
		log.Fatal(err)
	}
	session.Exec(fmt.Sprintf("create database %s", pgDB))
	if ok, err := databaseExists(pgDB); err != nil {
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
	UNIQUE(metric, value, timestamp, tags)
)
	`, metricsTable))

	session.Exec(fmt.Sprintf(`
	CREATE TABLE %s (
		id serial,
		metric text,
		out_metric text,
		run_every bigint,
		last_downsampled_window bigint,
		query jsonb
	)
		`, downsamplersTable))

	if ok, err := tableExists(metricsTable); err != nil {
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("could not create %s table", metricsTable)
	}

	if ok, err := tableExists(downsamplersTable); err != nil {
		log.Fatal(err)
	} else if !ok {
		log.Fatalf("could not create %s table", downsamplersTable)
	}

	downsamplers, err = selectDownsamplers()
	if err != nil {
		log.Fatal(err)
	}

	for _, d := range downsamplers {
		go waitDownsample(d)
	}
}

func waitDownsample(d *downsampler) {
	for {
		if d.Deleted {
			return
		}
		t0 := time.Now()
		err := downsample(d)
		if err != nil {
			log.Error(err)
		}
		t1 := time.Since(t0)
		log.Infof("downsample %d took %dms", d.ID, t1.Milliseconds())
		<-time.After(d.RunEveryDur)
	}
}
