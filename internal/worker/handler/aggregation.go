package handler

import (
	"encoding/json"
	"log"
	"sync"

	"zenity/internal/shared/event"
)

// Aggregation keeps rolling like/repost counts per subject and alerts on a
// threshold crossing (unusual traction).
type Aggregation struct {
	mu        sync.Mutex
	counts    map[string]int
	threshold int
	alert     func(subject string, n int) // injectable for tests; defaults to a log line
}

func NewAggregation(threshold int) *Aggregation {
	return &Aggregation{
		counts:    make(map[string]int),
		threshold: threshold,
		alert: func(subject string, n int) {
			log.Printf("[aggregation] %s crossed %d", subject, n)
		},
	}
}

func (a *Aggregation) Name() string { return "aggregation" }

func (a *Aggregation) Handle(e event.Event) error {
	if e.Commit == nil {
		return nil
	}
	subject := subjectURI(e.Commit.Record)
	if subject == "" {
		return nil
	}
	a.mu.Lock()
	a.counts[subject]++
	n := a.counts[subject]
	a.mu.Unlock()

	if n == a.threshold {
		a.alert(subject, n)
	}
	return nil
}

// subjectURI pulls record.subject.uri out of the raw record bytes
// (likes and reposts reference the liked/reposted post by URI).
func subjectURI(raw []byte) string {
	var r struct {
		Subject struct {
			URI string `json:"uri"`
		} `json:"subject"`
	}
	_ = json.Unmarshal(raw, &r)
	return r.Subject.URI
}
