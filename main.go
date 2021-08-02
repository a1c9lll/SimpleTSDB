package main

import (
	"flag"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetLevel(log.InfoLevel)
	configLocation := flag.String("config", "./config/simpletsdb.conf", "path to the configuration file")
	flag.Parse()

	log.Info("Starting SimpleTSDB")
	// load config
	cfg := map[string]string{}
	if err := loadConfig(*configLocation, cfg); err != nil {
		log.Fatal(err)
	}
	// parse db variables
	if v, ok := cfg["postgres_username"]; v == "" || !ok {
		log.Fatal("postgres_username config is required")
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
	if v, ok := cfg["postgres_n_conn_workers"]; v == "" || !ok {
		log.Fatal("postgres_n_conn_workers config is required")
	}
	var pgPassword string
	if p, ok := cfg["postgres_password"]; ok {
		pgPassword = p
	}

	dbPort, err := strconv.Atoi(cfg["postgres_port"])
	if err != nil {
		log.Fatal(err)
	}

	nConnWorkers, err := strconv.Atoi(cfg["postgres_n_conn_workers"])
	if err != nil {
		log.Fatal(err)
	}

	// parse server variables
	if v, ok := cfg["simpletsdb_bind_host"]; v == "" || !ok {
		log.Fatal("simpletsdb_bind_host config is required")
	}
	if v, ok := cfg["simpletsdb_bind_port"]; v == "" || !ok {
		log.Fatal("simpletsdb_bind_port config is required")
	}
	if v, ok := cfg["simpletsdb_http_read_timeout"]; v == "" || !ok {
		log.Fatal("simpletsdb_http_read_timeout config is required")
	}
	if v, ok := cfg["simpletsdb_http_write_timeout"]; v == "" || !ok {
		log.Fatal("simpletsdb_http_write_timeout config is required")
	}
	if v, ok := cfg["simpletsdb_line_buffer_size"]; v == "" || !ok {
		log.Fatal("simpletsdb_line_buffer_size config is required")
	}

	serverPort, err := strconv.Atoi(cfg["simpletsdb_bind_port"])
	if err != nil {
		log.Fatal(err)
	}
	serverReadTimeout, err := time.ParseDuration(cfg["simpletsdb_http_read_timeout"])
	if err != nil {
		log.Fatal(err)
	}
	serverWriteTimeout, err := time.ParseDuration(cfg["simpletsdb_http_write_timeout"])
	if err != nil {
		log.Fatal(err)
	}

	readLineProtocolBufferSize, err := strconv.Atoi(cfg["simpletsdb_line_buffer_size"])
	if err != nil {
		log.Fatal(err)
	}

	if v, ok := cfg["simpletsdb_insert_batch_size"]; v == "" || !ok {
		log.Fatal("simpletsdb_insert_batch_size config is required")
	}

	insertBatchSize, err = strconv.Atoi(cfg["simpletsdb_insert_batch_size"])
	if err != nil {
		log.Fatal(err)
	}

	// init db
	db := initDB(cfg["postgres_username"], pgPassword, cfg["postgres_host"], dbPort, cfg["postgres_db"], cfg["postgres_ssl_mode"], nConnWorkers)
	log.Infof("Connected to database [%s] at %s:%d", cfg["postgres_db"], cfg["postgres_host"], dbPort)

	// init server
	log.Infof("Initializing server at %s:%d", cfg["simpletsdb_bind_host"], serverPort)
	initServer(db, cfg["simpletsdb_bind_host"], serverPort, serverReadTimeout, serverWriteTimeout, readLineProtocolBufferSize)
}
