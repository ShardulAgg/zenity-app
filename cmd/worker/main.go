package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"zenity/internal/shared/broker"
	"zenity/internal/shared/event"
	"zenity/internal/worker/handler"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	addr := envOr("BROKER_ADDR", "localhost:9000")

	nc, err := broker.Connect(addr)
	if err != nil {
		log.Fatalf("[worker] connect broker %s: %v", addr, err)
	}
	defer nc.Close()
	log.Printf("[worker] connected to broker at %s", addr)

	// Map each subject to the handler that owns it.
	handlers := map[string]handler.Handler{
		"bsky.notification": handler.NewNotification([]string{"golang", "kubernetes"}),
		"bsky.aggregation":  handler.NewAggregation(50),
		"bsky.burst":        handler.NewBurst(10*time.Second, 20),
		"bsky.cleanup":      handler.NewCleanup(),
	}

	// Subscribe each handler. The queue group (the handler's name) lets several
	// replicas of this worker share one subject's load without double-processing.
	for subject, h := range handlers {
		err := nc.Subscribe(subject, h.Name(), func(e event.Event) {
			if err := h.Handle(e); err != nil {
				log.Printf("[%s] handle error: %v", h.Name(), err)
			}
		})
		if err != nil {
			log.Fatalf("[worker] subscribe %s: %v", subject, err)
		}
		log.Printf("[worker] subscribed %s -> %s", subject, h.Name())
	}

	log.Println("[worker] running…")
	<-ctx.Done()
	log.Println("[worker] shutting down")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
