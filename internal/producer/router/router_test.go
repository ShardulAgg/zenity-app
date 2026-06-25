package router

import (
	"testing"

	"zenity/internal/shared/event"
)

func commit(coll, op string) event.Event {
	return event.Event{Kind: "commit", Commit: &event.Commit{Collection: coll, Operation: op}}
}

func TestSubjectFor(t *testing.T) {
	cases := []struct {
		name string
		e    event.Event
		want string
	}{
		{"post create -> notification", commit("app.bsky.feed.post", "create"), "bsky.notification"},
		{"like create -> aggregation", commit("app.bsky.feed.like", "create"), "bsky.aggregation"},
		{"repost create -> aggregation", commit("app.bsky.feed.repost", "create"), "bsky.aggregation"},
		{"follow create -> burst", commit("app.bsky.graph.follow", "create"), "bsky.burst"},
		{"like delete -> cleanup", commit("app.bsky.feed.like", "delete"), "bsky.cleanup"},
		{"post delete -> cleanup", commit("app.bsky.feed.post", "delete"), "bsky.cleanup"},
		{"unhandled collection -> dropped", commit("app.bsky.graph.block", "create"), ""},
		{"identity -> dropped", event.Event{Kind: "identity"}, ""},
		{"account -> dropped", event.Event{Kind: "account"}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := subjectFor(c.e); got != c.want {
				t.Errorf("subjectFor() = %q, want %q", got, c.want)
			}
		})
	}
}

// fakePublisher records the subjects the router publishes to.
type fakePublisher struct{ calls []string }

func (f *fakePublisher) Publish(subject string, _ event.Event) error {
	f.calls = append(f.calls, subject)
	return nil
}

func TestDispatchPublishesRoutedDropsRest(t *testing.T) {
	f := &fakePublisher{}
	r := New(f)
	r.Dispatch(commit("app.bsky.feed.like", "create"))   // -> bsky.aggregation
	r.Dispatch(event.Event{Kind: "identity"})            // dropped (no commit)
	r.Dispatch(commit("app.bsky.graph.block", "create")) // dropped (unhandled)

	if len(f.calls) != 1 || f.calls[0] != "bsky.aggregation" {
		t.Fatalf("expected one publish to bsky.aggregation, got %v", f.calls)
	}
}
