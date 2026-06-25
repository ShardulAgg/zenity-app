package handler

import "zenity/internal/shared/event"

// Handler processes one class of event. Each implementation owns its state.
type Handler interface {
	Name() string
	Handle(e event.Event) error
}
