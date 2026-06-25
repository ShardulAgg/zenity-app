package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"zenity/internal/producer/jetstream"
	"zenity/internal/producer/router"
	"zenity/internal/shared/broker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addr := envOr("BROKER_ADDR", "localhost:9000")

	// 1. Connect to the broker.
	nc, err := broker.Connect(addr)
	if err != nil {
		log.Fatalf("[ingest] connect broker %s: %v", addr, err)
	}
	defer nc.Close()
	log.Printf("[ingest] connected to broker at %s", addr)

	// 2. Build the router, publishing through NATS.
	r := router.New(nc)

	// 3. Stream from Jetstream into the router. Subscribe only to the
	//    collections we route (server-side filter).
	collections := []string{
		"app.bsky.feed.post",
		"app.bsky.feed.like",
		"app.bsky.feed.repost",
		"app.bsky.graph.follow",
	}

	log.Println("[ingest] streaming from Jetstream…")
	if err := jetstream.Stream(ctx, collections, r.Dispatch); err != nil {
		log.Printf("[ingest] stopped: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
