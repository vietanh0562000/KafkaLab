package events

import (
	"testing"
	"time"
)

func TestPublishNeverBlocksOnSlowSubscriber(t *testing.T) {
	h := NewHub()
	_, cancel := h.Subscribe() // never read
	defer cancel()
	fast, c2 := h.Subscribe()
	defer c2()
	go func() {
		for range fast {
		}
	}()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			h.Publish(Event{Type: "log"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("publish blocked")
	}
}

func TestSubscriberReceivesAndUnsubscribes(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe()
	h.Publish(Event{Type: "step", Data: 1})
	if e := <-ch; e.Type != "step" || e.At.IsZero() {
		t.Fatalf("%+v", e)
	}
	cancel()
	if _, open := <-ch; open {
		t.Fatal("channel must close on cancel")
	}
	h.Publish(Event{Type: "log"}) // must not panic after unsubscribe
}
