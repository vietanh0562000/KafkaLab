package web

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"kafkalab/internal/chaos"
	"kafkalab/internal/events"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
	"kafkalab/internal/scenario"
)

type fakeBackend struct {
	startErr, chaosErr error
}

func (f *fakeBackend) Summaries() []scenario.Summary { return []scenario.Summary{{ID: "0.1"}} }
func (f *fakeBackend) Start(string) error            { return f.startErr }
func (f *fakeBackend) Reset() error                  { return nil }
func (f *fakeBackend) Produce(context.Context, lab.ProduceReq) error {
	return nil
}
func (f *fakeBackend) Chaos(context.Context, string, string) error { return f.chaosErr }
func (f *fakeBackend) Snapshot() observe.Snapshot                  { return observe.Snapshot{DBRows: 3} }

var static = fstest.MapFS{"index.html": {Data: []byte("<title>Kafka Lab</title>")}}

func serve(b Backend, hub *events.Hub) *httptest.Server {
	return httptest.NewServer(NewServer(b, hub, static, nil))
}

func post(t *testing.T, url, body string) int {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestRunBusyIs409(t *testing.T) {
	s := serve(&fakeBackend{startErr: scenario.ErrBusy}, events.NewHub())
	defer s.Close()
	if code := post(t, s.URL+"/api/scenarios/0.1/run", ""); code != http.StatusConflict {
		t.Fatal(code)
	}
}

func TestUnknownRunIs404(t *testing.T) {
	s := serve(&fakeBackend{startErr: scenario.ErrUnknown}, events.NewHub())
	defer s.Close()
	if code := post(t, s.URL+"/api/scenarios/nope/run", ""); code != http.StatusNotFound {
		t.Fatal(code)
	}
}

func TestChaosNotLabIs403(t *testing.T) {
	s := serve(&fakeBackend{chaosErr: chaos.ErrNotLab}, events.NewHub())
	defer s.Close()
	if code := post(t, s.URL+"/api/chaos", `{"action":"stop","target":"timescaledb"}`); code != http.StatusForbidden {
		t.Fatal(code)
	}
}

func TestSSEDeliversPublishedEvent(t *testing.T) {
	hub := events.NewHub()
	s := serve(&fakeBackend{}, hub)
	defer s.Close()
	resp, err := http.Get(s.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	go func() {
		time.Sleep(100 * time.Millisecond) // let the handler subscribe
		hub.Publish(events.Event{Type: "snapshot", Data: map[string]int{"dbRows": 7}})
	}()
	sc := bufio.NewScanner(resp.Body)
	sawEvent := false
	deadline := time.After(2 * time.Second)
	lines := make(chan string)
	go func() {
		for sc.Scan() {
			lines <- sc.Text()
		}
	}()
	for {
		select {
		case l := <-lines:
			if l == "event: snapshot" {
				sawEvent = true
			} else if sawEvent && strings.HasPrefix(l, "data: ") && strings.Contains(l, `"dbRows":7`) {
				return
			}
		case <-deadline:
			t.Fatal("no snapshot event")
		}
	}
}

func TestIndexServed(t *testing.T) {
	s := serve(&fakeBackend{}, events.NewHub())
	defer s.Close()
	resp, err := http.Get(s.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 100)
	n, _ := resp.Body.Read(buf)
	if resp.StatusCode != 200 || !strings.Contains(string(buf[:n]), "Kafka Lab") {
		t.Fatal(resp.StatusCode, string(buf[:n]))
	}
}

func TestSnapshotEndpoint(t *testing.T) {
	s := serve(&fakeBackend{}, events.NewHub())
	defer s.Close()
	resp, err := http.Get(s.URL + "/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 2000)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), `"dbRows":3`) {
		t.Fatal(string(buf[:n]))
	}
}

func TestStaticNotCached(t *testing.T) {
	s := serve(&fakeBackend{}, events.NewHub())
	defer s.Close()
	resp, err := http.Get(s.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// The page is embedded in the binary; after a rebuild the browser must not keep the old one.
	if got := resp.Header.Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control %q", got)
	}
}

func TestPostRequiresJSONContentType(t *testing.T) {
	s := serve(&fakeBackend{}, events.NewHub())
	defer s.Close()
	// A page on another origin can send text/plain without a CORS preflight; it must not reach chaos or reset.
	for _, path := range []string{"/api/chaos", "/api/reset", "/api/produce", "/api/scenarios/0.1/run"} {
		resp, err := http.Post(s.URL+path, "text/plain", strings.NewReader(`{"action":"stop","target":"lab-kafka-1"}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnsupportedMediaType {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
}
