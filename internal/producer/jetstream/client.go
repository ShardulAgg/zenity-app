package jetstream

import (
	"context"
	"log"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"zenity/internal/shared/event"
)

const host = "jetstream2.us-east.bsky.network"

// Stream connects to Jetstream and calls fn for every decoded event,
// reconnecting and resuming from the last cursor on disconnect.
func Stream(ctx context.Context, collections []string, fn func(event.Event)) error {
	var cursor int64
	for ctx.Err() == nil {
		cursor = readLoop(ctx, collections, cursor, fn)
		if ctx.Err() != nil {
			break // shutting down, not a real disconnect
		}
		log.Printf("[jetstream] disconnected, resuming from cursor %d", cursor)
		select {
		case <-ctx.Done():
		case <-time.After(time.Second): // simple backoff
		}
	}
	return ctx.Err()
}

// readLoop runs one connection until it errors, returning the latest cursor.
func readLoop(ctx context.Context, collections []string, cursor int64, fn func(event.Event)) int64 {
	u := url.URL{Scheme: "wss", Host: host, Path: "/subscribe"}
	q := u.Query()
	for _, c := range collections {
		q.Add("wantedCollections", c)
	}
	if cursor > 0 {
		q.Set("cursor", strconv.FormatInt(cursor, 10))
	}
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		log.Printf("[jetstream] dial: %v", err)
		return cursor
	}
	defer conn.Close()

	// Close the connection when the context is cancelled so the blocking
	// ReadMessage below unblocks and the loop can exit.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	log.Printf("[jetstream] connected: %s", u.String())

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return cursor // bubble up -> reconnect
		}
		e, err := event.Decode(data)
		if err != nil {
			continue
		}
		cursor = e.TimeUS // remember position for resume
		fn(e)
	}
}
