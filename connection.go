package main

import (
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"

	_ "github.com/lib/pq"
)

var (
	session *sql.DB
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

	connStr := fmt.Sprintf("user=%s %shost='%s' port=%d dbname=%s sslmode=%s", pgUser, passwordString, pgHost, pgPort, pgDB, pgSSLMode)
	session, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	session.Exec(`
CREATE TABLE simpletsdb_metrics (
	metric text,
	value double precision,
	timestamp bigint,
	tags jsonb,
	UNIQUE(metric, value, timestamp, tags)
)
	`)
	if err := session.Ping(); err != nil {
		log.Fatal(err)
	}
}
