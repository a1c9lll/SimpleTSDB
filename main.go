package main

import (
	"strconv"
	"time"

	"simpletsdb/datastore"
	"simpletsdb/server"
	"simpletsdb/util"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.Info("Starting SimpleTSDB")
	// load config
	cfg := map[string]string{}
	if err := util.LoadConfig("config", cfg); err != nil {
		log.Fatal(err)
	}
	// init db
	if v, ok := cfg["postgres_username"]; v == "" || !ok {
		log.Fatal("postgres_username config is required")
	}
	if v, ok := cfg["postgres_username"]; v == "" || !ok {
		log.Fatal("postgres_password config is required")
	}
	if v, ok := cfg["postgres_ssl_mode"]; v == "" || !ok {
		log.Fatal("postgres_ssl_mode config is required")
	}
	if v, ok := cfg["postgres_host"]; v == "" || !ok {
		log.Fatal("postgres_host config is required")
	}
	if v, ok := cfg["postgres_port"]; v == "" || !ok {
		log.Fatal("postgres_port config is required")
	}
	if v, ok := cfg["postgres_db"]; v == "" || !ok {
		log.Fatal("postgres_db config is required")
	}

	dbPort, err := strconv.Atoi(cfg["postgres_port"])
	if err != nil {
		log.Fatal(err)
	}
	datastore.InitDB(cfg["postgres_username"], cfg["postgres_password"], cfg["postgres_host"], dbPort, cfg["postgres_db"], cfg["postgres_ssl_mode"])

	log.Infof("Connected to database [%s] at %s:%d", cfg["postgres_db"], cfg["postgres_host"], dbPort)
	// init server
	if v, ok := cfg["simpletsdb_bind_host"]; v == "" || !ok {
		log.Fatal("simpletsdb_bind_host config is required")
	}
	if v, ok := cfg["simpletsdb_bind_port"]; v == "" || !ok {
		log.Fatal("simpletsdb_bind_port config is required")
	}
	if v, ok := cfg["simpletsdb_read_timeout"]; v == "" || !ok {
		log.Fatal("simpletsdb_read_timeout config is required")
	}
	if v, ok := cfg["simpletsdb_write_timeout"]; v == "" || !ok {
		log.Fatal("simpletsdb_write_timeout config is required")
	}
	if v, ok := cfg["simpletsdb_line_buffer_size"]; v == "" || !ok {
		log.Fatal("simpletsdb_line_buffer_size config is required")
	}
	serverPort, err := strconv.Atoi(cfg["simpletsdb_bind_port"])
	if err != nil {
		log.Fatal(err)
	}
	serverReadTimeout, err := time.ParseDuration(cfg["simpletsdb_read_timeout"])
	if err != nil {
		log.Fatal(err)
	}
	serverWriteTimeout, err := time.ParseDuration(cfg["simpletsdb_write_timeout"])
	if err != nil {
		log.Fatal(err)
	}

	readLineProtocolBufferSize, err := strconv.Atoi(cfg["simpletsdb_line_buffer_size"])
	if err != nil {
		log.Fatal(err)
	}

	log.Infof("Initializing server at %s:%d", cfg["simpletsdb_bind_host"], serverPort)
	server.Init(cfg["simpletsdb_bind_host"], serverPort, serverReadTimeout, serverWriteTimeout, readLineProtocolBufferSize)
}
