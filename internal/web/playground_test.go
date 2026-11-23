package web

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"kafkalab/internal/events"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/playground"
)

type fakePG struct {
	PG
	prof     kafkax.ProducerProfile
	deleted  string
	sent     chan []any
	hold     chan struct{}
	stateFor string
}

func (f *fakePG) AddProducer(name string, p kafkax.ProducerProfile) error {
	f.prof = p
	if name == "dup" {
		return errors.New("producer dup already exists")
	}
	return nil
}
func (f *fakePG) DeleteTopic(_ context.Context, name string) error { f.deleted = name; return nil }
func (f *fakePG) Send(_ context.Context, p, t, k, v string, n int) ([]playground.SendView, error) {
	if p == "nope" {
		return nil, errors.New("no producer nope")
	}
	if f.hold != nil {
		<-f.hold
	}
	f.sent <- []any{p, t, k, v, n}
	return []playground.SendView{{Topic: t, Key: k, Partition: 2, Offset: 7}}, nil
}
func (f *fakePG) State(_ context.Context, topic string) playground.State {
	f.stateFor = topic
	return playground.State{Errors: map[string]string{}}
}

func pgServer(pg PG) *httptest.Server {
	return httptest.NewServer(NewServer(&fakeBackend{}, events.NewHub(), static, pg))
}

func call(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestPlaygroundProducerProfileFromJSON(t *testing.T) {
	f := &fakePG{}
	s := pgServer(f)
	defer s.Close()
	code, _ := call(t, "POST", s.URL+"/api/pg/producers", `{"name":"p2","acks":"1","idempotent":false}`)
	if code != http.StatusCreated || f.prof.Acks != "1" || f.prof.Idempotent {
		t.Fatalf("%d %+v", code, f.prof)
	}
	code, body := call(t, "POST", s.URL+"/api/pg/producers", `{"name":"dup","acks":"all","idempotent":true}`)
	if code != http.StatusBadRequest || !strings.Contains(body, "already exists") {
		t.Fatalf("%d %s", code, body)
	}
}

func TestPlaygroundSendAnswersBeforeTheBrokerDoes(t *testing.T) {
	f := &fakePG{sent: make(chan []any, 1), hold: make(chan struct{})}
	s := pgServer(f)
	defer s.Close()
	// A send waiting on missing replicas must not hold the request; its result lands in the producer's sends.
	code, body := call(t, "POST", s.URL+"/api/pg/producers/p1/send", `{"topic":"orders","key":"xe-1","value":"v","count":3,"acks":"all"}`)
	if code != http.StatusAccepted || !strings.Contains(body, "kafka-console-producer.sh") {
		t.Fatalf("%d %s", code, body)
	}
	close(f.hold)
	select {
	case args := <-f.sent:
		if args[0] != "p1" || args[1] != "orders" || args[4] != 3 {
			t.Fatal(args)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send never finished")
	}
	// Input errors come back at once, not as a silent 202.
	if code, body := call(t, "POST", s.URL+"/api/pg/producers/nope/send", `{"topic":"orders","count":1}`); code != http.StatusBadRequest {
		t.Fatalf("%d %s", code, body)
	}
}

func TestPlaygroundStateAndDelete(t *testing.T) {
	f := &fakePG{}
	s := pgServer(f)
	defer s.Close()
	if code, _ := call(t, "GET", s.URL+"/api/pg/state?topic=orders", ""); code != http.StatusOK || f.stateFor != "orders" {
		t.Fatal(code, f.stateFor)
	}
	if code, _ := call(t, "DELETE", s.URL+"/api/pg/topics/orders", ""); code != http.StatusOK || f.deleted != "orders" {
		t.Fatal(code, f.deleted)
	}
	if code, body := call(t, "GET", s.URL+"/api/pg/lessons", ""); code != http.StatusOK || !strings.Contains(body, `"id":"8"`) {
		t.Fatal(code, body)
	}
}
