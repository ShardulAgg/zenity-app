package router

import (
	"log"

	"zenity/internal/shared/event"
)

// Publisher hands a classified event to the transport — a broker in production,
// or an in-process channel in tests. The router doesn't care which.
type Publisher interface {
	Publish(subject string, e event.Event) error
}

// routes maps a route key to the subject its events are published on. Likes and
// reposts deliberately share one subject (a single aggregation worker handles both).
var routes = map[string]string{
	"commit|app.bsky.feed.post|create":    "bsky.notification",
	"commit|app.bsky.feed.like|create":    "bsky.aggregation",
	"commit|app.bsky.feed.repost|create":  "bsky.aggregation",
	"commit|app.bsky.graph.follow|create": "bsky.burst",
}

// Router classifies events and publishes them onto per-type subjects.
type Router struct {
	pub Publisher
}

func New(pub Publisher) *Router {
	return &Router{pub: pub}
}

// Dispatch classifies one event and publishes it to its subject. Unroutable
// events (identity/account, unhandled collections) are dropped.
func (r *Router) Dispatch(e event.Event) {
	subject := subjectFor(e)
	if subject == "" {
		return
	}
	if err := r.pub.Publish(subject, e); err != nil {
		log.Printf("[router] publish %s: %v", subject, err)
	}
}

// subjectFor applies the routing precedence: kind first (identity/account have
// no commit), then the delete rule, then an exact route-key match.
func subjectFor(e event.Event) string {
	if e.Kind != "commit" {
		return ""
	}
	if e.Commit.Operation == "delete" {
		return "bsky.cleanup"
	}
	return routes[e.RouteKey()]
}
