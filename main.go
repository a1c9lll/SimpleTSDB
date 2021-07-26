package main

import (
	"log"
	"strconv"
	"time"

	"simpletsdb/datastore"
	"simpletsdb/server"
	"simpletsdb/util"
)

func main() {
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
	if v, ok := cfg["postgress_ssl_mode"]; v == "" || !ok {
		log.Fatal("postgress_ssl_mode config is required")
	}
	if v, ok := cfg["postgress_host"]; v == "" || !ok {
		log.Fatal("postgress_host config is required")
	}
	if v, ok := cfg["postgress_port"]; v == "" || !ok {
		log.Fatal("postgress_port config is required")
	}

	dbPort, err := strconv.Atoi(cfg["postgres_port"])
	if err != nil {
		log.Fatal(err)
	}
	datastore.InitDB(cfg["postgres_username"], cfg["postgres_password"], cfg["postgres_password"], dbPort, cfg["postgress_ssl_mode"])

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

	server.Init(cfg["simpletsdb_bind_host"], serverPort, serverReadTimeout, serverWriteTimeout, readLineProtocolBufferSize)
}
