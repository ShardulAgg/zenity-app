package handler

import (
	"encoding/json"
	"log"
	"strings"

	"zenity/internal/shared/event"
)

// Notification matches new posts against a keyword set and raises an alert on
// a hit. Stateless — one event in, a decision out.
type Notification struct {
	keywords []string
}

func NewNotification(keywords []string) *Notification {
	return &Notification{keywords: keywords}
}

func (n *Notification) Name() string { return "notification" }

func (n *Notification) Handle(e event.Event) error {
	if e.Commit == nil {
		return nil
	}
	var post struct {
		Text  string   `json:"text"`
		Langs []string `json:"langs"`
	}
	if err := json.Unmarshal(e.Commit.Record, &post); err != nil {
		return err
	}
	text := strings.ToLower(post.Text)
	for _, kw := range n.keywords {
		if strings.Contains(text, strings.ToLower(kw)) {
			log.Printf("[notification] hit %q in post by %s", kw, e.Did)
			return nil
		}
	}
	return nil
}
