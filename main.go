package main

import (
	"log"
	"strconv"

	"simpletsdb/datastore"
	"simpletsdb/util"
)

func main() {

}

func init() {
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
	port, err := strconv.Atoi(cfg["postgres_port"])
	if err != nil {
		log.Fatal(err)
	}
	datastore.InitDB(cfg["postgres_username"], cfg["postgres_password"], cfg["postgres_password"], port, cfg["postgress_ssl_mode"])
}
