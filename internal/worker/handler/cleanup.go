package handler

import (
	"log"

	"zenity/internal/shared/event"
)

// Cleanup handles every delete. Deletes carry no record — only the collection
// and rkey identify what to retract.
type Cleanup struct{}

func NewCleanup() *Cleanup { return &Cleanup{} }

func (c *Cleanup) Name() string { return "cleanup" }

func (c *Cleanup) Handle(e event.Event) error {
	if e.Commit == nil {
		return nil
	}
	log.Printf("[cleanup] retract %s/%s by %s", e.Commit.Collection, e.Commit.RKey, e.Did)
	return nil
}
