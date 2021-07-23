package datastore

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

var (
	session *sql.DB
)

func InitDB(pgUser, pgPassword, pgHost string, pgPort int, pgSSLMode string) {
	connStr0 := fmt.Sprintf("user=%s password='%s' host='%s' port=%d sslmode=%s", pgUser, pgPassword, pgHost, pgPort, pgSSLMode)
	var err error
	session, err = sql.Open("postgres", connStr0)
	if err != nil {
		log.Fatal(err)
	}
	session.Query("create database simpletsdb")
	if err := session.Close(); err != nil {
		log.Fatal(err)
	}
	connStr := fmt.Sprintf("user=%s password='%s' host='%s' dbname=simpletsdb port=%d sslmode=%s", pgUser, pgPassword, pgHost, pgPort, pgSSLMode)
	session, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
}
