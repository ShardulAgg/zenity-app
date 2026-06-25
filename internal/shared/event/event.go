package event

import "encoding/json"

// Event is one decoded Jetstream message.
type Event struct {
	Did    string  `json:"did"`
	Kind   string  `json:"kind"`    // commit | identity | account
	TimeUS int64   `json:"time_us"` // cursor position for resume
	Commit *Commit `json:"commit,omitempty"`
}

// Commit is the repo-write payload — nil for identity/account events.
type Commit struct {
	Operation  string          `json:"operation"`  // create | update | delete
	Collection string          `json:"collection"` // e.g. app.bsky.feed.post
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
}

// RouteKey is the discriminator the router switches on.
func (e Event) RouteKey() string {
	if e.Commit == nil {
		return e.Kind
	}
	return e.Kind + "|" + e.Commit.Collection + "|" + e.Commit.Operation
}

// Decode parses one raw Jetstream frame.
func Decode(b []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(b, &e)
	return e, err
}
