// Package events fans lab events out to every open dashboard.
package events

import (
	"sync"
	"time"
)

type Event struct {
	Type string    `json:"type"` // snapshot | log | step | scenario
	At   time.Time `json:"at"`
	Data any       `json:"data"`
}

type Hub struct {
	mu   sync.Mutex
	subs map[chan Event]struct{}
}

func NewHub() *Hub { return &Hub{subs: map[chan Event]struct{}{}} }

// Publish drops the event for a subscriber whose buffer is full, so a stalled browser tab never blocks the lab.
func (h *Hub) Publish(e Event) {
	if e.At.IsZero() {
		e.At = time.Now()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- e:
		default:
		}
	}
}

func (h *Hub) Subscribe() (<-chan Event, func()) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs, ch)
			h.mu.Unlock()
			close(ch)
		})
	}
}
