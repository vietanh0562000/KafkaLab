// Package web serves the dashboard, its JSON API and the SSE event stream.
package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"net/http"
	"time"

	"kafkalab/internal/chaos"
	"kafkalab/internal/events"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
	"kafkalab/internal/scenario"
)

type Backend interface {
	Summaries() []scenario.Summary
	Start(id string) error
	Reset() error
	Produce(ctx context.Context, r lab.ProduceReq) error
	Chaos(ctx context.Context, action, target string) error
	Snapshot() observe.Snapshot
}

func NewServer(b Backend, hub *events.Hub, static fs.FS, pg PG) http.Handler {
	mux := http.NewServeMux()
	if pg != nil {
		routePlayground(mux, pg)
	}
	files := http.FileServerFS(static)
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
	mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) { stream(w, r, hub) })
	mux.HandleFunc("GET /api/snapshot", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, b.Snapshot()) })
	mux.HandleFunc("GET /api/scenarios", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, b.Summaries()) })
	mux.HandleFunc("POST /api/scenarios/{id}/run", func(w http.ResponseWriter, r *http.Request) {
		respond(w, b.Start(r.PathValue("id")), http.StatusAccepted)
	})
	mux.HandleFunc("POST /api/reset", func(w http.ResponseWriter, r *http.Request) {
		respond(w, b.Reset(), http.StatusAccepted)
	})
	mux.HandleFunc("POST /api/produce", func(w http.ResponseWriter, r *http.Request) {
		var req lab.ProduceReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Produce can take minutes at a low rate; the outcome goes to the event log.
		go b.Produce(context.Background(), req)
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("POST /api/chaos", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Action, Target string }
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		respond(w, b.Chaos(ctx, req.Action, req.Target), http.StatusOK)
	})
	return requireJSON(mux)
}

// requireJSON rejects POSTs that are not application/json: a page on another origin can send text/plain
// without a CORS preflight, and these endpoints stop containers.
func requireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mt != "application/json" {
				http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func respond(w http.ResponseWriter, err error, okStatus int) {
	switch {
	case err == nil:
		w.WriteHeader(okStatus)
	case errors.Is(err, scenario.ErrBusy):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, scenario.ErrUnknown):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, chaos.ErrNotLab):
		http.Error(w, err.Error(), http.StatusForbidden)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func stream(w http.ResponseWriter, r *http.Request, hub *events.Hub) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ch, cancel := hub.Subscribe()
	defer cancel()
	fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
		case e, open := <-ch:
			if !open {
				return
			}
			data, err := json.Marshal(e)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, data)
		}
		flusher.Flush()
	}
}
