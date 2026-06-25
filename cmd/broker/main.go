package main

import (
	"log"
	"os"

	"zenity/internal/shared/broker"
)

func main() {
	addr := os.Getenv("BROKER_LISTEN")
	if addr == "" {
		addr = ":9000"
	}
	srv := broker.NewServer(1024) // per-consumer send-queue depth
	log.Fatal(srv.ListenAndServe(addr))
}
