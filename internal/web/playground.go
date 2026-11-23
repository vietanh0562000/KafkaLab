package web

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"kafkalab/internal/kafkax"
	"kafkalab/internal/playground"
)

type PG interface {
	State(ctx context.Context, topic string) playground.State
	CreateTopic(ctx context.Context, r playground.TopicReq) error
	DeleteTopic(ctx context.Context, name string) error
	AddProducer(name string, p kafkax.ProducerProfile) error
	RemoveProducer(name string) error
	Send(ctx context.Context, producer, topic, key, value string, count int) ([]playground.SendView, error)
	AddConsumer(name, group, topic, reset string) error
	RemoveConsumer(name string) error
	Poll(ctx context.Context, name string, max int) ([]playground.RecordView, error)
	Commit(ctx context.Context, name string) error
	SetAuto(name string, on bool) error
	Clear(ctx context.Context) error
}

// decode reads a JSON body into v; it writes the 400 itself and reports whether to go on.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(w, "bad JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// reply writes {"cli": ..., "data": ...} or a 400 with the error text, which the UI shows as is.
func reply(w http.ResponseWriter, err error, status int, cli string, data any) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"cli": cli, "data": data})
}

func routePlayground(mux *http.ServeMux, pg PG) {
	bounded := func(r *http.Request) (context.Context, context.CancelFunc) {
		return context.WithTimeout(r.Context(), 30*time.Second)
	}
	mux.HandleFunc("GET /api/pg/state", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		writeJSON(w, pg.State(ctx, r.URL.Query().Get("topic")))
	})
	mux.HandleFunc("GET /api/pg/lessons", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, playground.Lessons) })
	mux.HandleFunc("POST /api/pg/topics", func(w http.ResponseWriter, r *http.Request) {
		var req playground.TopicReq
		if !decode(w, r, &req) {
			return
		}
		ctx, cancel := bounded(r)
		defer cancel()
		reply(w, pg.CreateTopic(ctx, req), http.StatusCreated, playground.TopicCLI(req), nil)
	})
	mux.HandleFunc("DELETE /api/pg/topics/{name}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := bounded(r)
		defer cancel()
		name := r.PathValue("name")
		reply(w, pg.DeleteTopic(ctx, name), http.StatusOK, playground.DeleteTopicCLI(name), nil)
	})
	mux.HandleFunc("POST /api/pg/producers", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name       string `json:"name"`
			Acks       string `json:"acks"`
			Idempotent bool   `json:"idempotent"`
		}
		if !decode(w, r, &req) {
			return
		}
		prof := kafkax.ProducerProfile{Acks: req.Acks, Idempotent: req.Idempotent}
		reply(w, pg.AddProducer(req.Name, prof), http.StatusCreated, "", nil)
	})
	mux.HandleFunc("DELETE /api/pg/producers/{name}", func(w http.ResponseWriter, r *http.Request) {
		reply(w, pg.RemoveProducer(r.PathValue("name")), http.StatusOK, "", nil)
	})
	mux.HandleFunc("POST /api/pg/producers/{name}/send", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Topic, Key, Value, Acks string
			Count                   int
		}
		if !decode(w, r, &req) {
			return
		}
		// A send waiting on missing replicas must not hold the request: after a short wait it answers 202 and the
		// outcome lands in the producer's sends. Input errors return within the wait.
		cli := playground.SendCLI(req.Topic, req.Key, req.Acks)
		done := make(chan error, 1)
		go func(name string) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			_, err := pg.Send(ctx, name, req.Topic, req.Key, req.Value, req.Count)
			done <- err
		}(r.PathValue("name"))
		select {
		case err := <-done:
			reply(w, err, http.StatusOK, cli, nil)
		case <-time.After(500 * time.Millisecond):
			reply(w, nil, http.StatusAccepted, cli, nil)
		}
	})
	mux.HandleFunc("POST /api/pg/consumers", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Name, Group, Topic, Reset string }
		if !decode(w, r, &req) {
			return
		}
		reply(w, pg.AddConsumer(req.Name, req.Group, req.Topic, req.Reset), http.StatusCreated,
			playground.ConsumerCLI(req.Group, req.Topic, req.Reset), nil)
	})
	mux.HandleFunc("DELETE /api/pg/consumers/{name}", func(w http.ResponseWriter, r *http.Request) {
		reply(w, pg.RemoveConsumer(r.PathValue("name")), http.StatusOK, "", nil)
	})
	mux.HandleFunc("POST /api/pg/consumers/{name}/poll", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Max int }
		if !decode(w, r, &req) {
			return
		}
		ctx, cancel := bounded(r)
		defer cancel()
		recs, err := pg.Poll(ctx, r.PathValue("name"), req.Max)
		reply(w, err, http.StatusOK, "", recs)
	})
	mux.HandleFunc("POST /api/pg/consumers/{name}/commit", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := bounded(r)
		defer cancel()
		reply(w, pg.Commit(ctx, r.PathValue("name")), http.StatusOK, "", nil)
	})
	mux.HandleFunc("POST /api/pg/consumers/{name}/auto", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ On bool }
		if !decode(w, r, &req) {
			return
		}
		reply(w, pg.SetAuto(r.PathValue("name"), req.On), http.StatusOK, "", nil)
	})
	mux.HandleFunc("POST /api/pg/clear", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := bounded(r)
		defer cancel()
		reply(w, pg.Clear(ctx), http.StatusOK, "", nil)
	})
}
