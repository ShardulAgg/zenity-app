package handler

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"zenity/internal/shared/event"
)

// Burst detects many follows for a single account within a sliding window.
type Burst struct {
	mu     sync.Mutex
	window time.Duration
	max    int
	hits   map[string][]time.Time // subject DID -> recent follow times
}

func NewBurst(window time.Duration, max int) *Burst {
	return &Burst{window: window, max: max, hits: make(map[string][]time.Time)}
}

func (b *Burst) Name() string { return "burst" }

func (b *Burst) Handle(e event.Event) error {
	if e.Commit == nil {
		return nil
	}
	subject := subjectDID(e.Commit.Record)
	if subject == "" {
		return nil
	}
	now := time.Now()
	cutoff := now.Add(-b.window)

	b.mu.Lock()
	// keep only timestamps inside the window, then add the new one
	times := append(b.hits[subject], now)
	kept := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	b.hits[subject] = kept
	n := len(kept)
	b.mu.Unlock()

	if n == b.max {
		log.Printf("[burst] %s reached %d follows within %s", subject, n, b.window)
	}
	return nil
}

// subjectDID pulls the bare DID string from a follow record's subject.
func subjectDID(raw []byte) string {
	var r struct {
		Subject string `json:"subject"`
	}
	_ = json.Unmarshal(raw, &r)
	return r.Subject
}
