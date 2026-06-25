package handler

import (
	"encoding/json"
	"testing"

	"zenity/internal/shared/event"
)

func likeOf(uri string) event.Event {
	rec, _ := json.Marshal(map[string]any{"subject": map[string]string{"uri": uri}})
	return event.Event{Kind: "commit", Commit: &event.Commit{
		Collection: "app.bsky.feed.like", Operation: "create", Record: rec,
	}}
}

func TestAggregationThresholdFiresOnce(t *testing.T) {
	a := NewAggregation(3)
	var alerts int
	a.alert = func(string, int) { alerts++ }

	uri := "at://did/app.bsky.feed.post/abc"
	for i := 0; i < 5; i++ { // cross the threshold and keep going
		if err := a.Handle(likeOf(uri)); err != nil {
			t.Fatal(err)
		}
	}
	if alerts != 1 {
		t.Errorf("expected exactly 1 alert at threshold, got %d", alerts)
	}
}

func TestAggregationCountsPerSubject(t *testing.T) {
	a := NewAggregation(2)
	var fired []string
	a.alert = func(subject string, _ int) { fired = append(fired, subject) }

	a.Handle(likeOf("post-A"))
	a.Handle(likeOf("post-B"))
	a.Handle(likeOf("post-A")) // post-A reaches 2 -> fires; post-B is at 1

	if len(fired) != 1 || fired[0] != "post-A" {
		t.Errorf("expected post-A to fire once, got %v", fired)
	}
}
