package scenario

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"kafkalab/internal/events"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
)

type sink struct {
	mu  sync.Mutex
	evs []events.Event
}

func (s *sink) emit(e events.Event) {
	s.mu.Lock()
	s.evs = append(s.evs, e)
	s.mu.Unlock()
}

func (s *sink) statuses() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, e := range s.evs {
		if se, ok := e.Data.(StepEvent); ok {
			out = append(out, se.Step+":"+se.Status)
		}
	}
	return out
}

func noReset(context.Context, lab.ResetOpts) error { return nil }
func emptySnap() observe.Snapshot                  { return observe.Snapshot{} }

func TestPassingScenarioEmitsSteps(t *testing.T) {
	s := &sink{}
	sc := Scenario{ID: "a", Steps: []Step{
		Action("do", func(context.Context, *Ctx) error { return nil }),
		Expect("check", func(observe.Snapshot, *Ctx) (bool, string) { return true, "ok" }),
	}}
	r := newRunner(noReset, emptySnap, s.emit, []Scenario{sc})
	res, err := r.RunSync(context.Background(), "a")
	if err != nil || !res.Passed || len(res.Steps) != 3 {
		t.Fatalf("%+v %v", res, err)
	}
	want := "Baseline:running Baseline:pass do:running do:pass check:running check:pass"
	if got := strings.Join(s.statuses(), " "); got != want {
		t.Fatalf("got %s", got)
	}
}

func TestWaitUntilTimesOut(t *testing.T) {
	sc := Scenario{ID: "a", Steps: []Step{
		WaitUntil("never", 300*time.Millisecond, func(observe.Snapshot, *Ctx) bool { return false }),
	}}
	r := newRunner(noReset, emptySnap, func(events.Event) {}, []Scenario{sc})
	res, _ := r.RunSync(context.Background(), "a")
	if res.Passed || !strings.Contains(res.Steps[1].Observed, "timeout") {
		t.Fatalf("%+v", res)
	}
}

func TestFailureStopsRemainingSteps(t *testing.T) {
	ran := false
	sc := Scenario{ID: "a", Steps: []Step{
		Action("boom", func(context.Context, *Ctx) error { return errors.New("boom") }),
		Action("after", func(context.Context, *Ctx) error { ran = true; return nil }),
	}}
	r := newRunner(noReset, emptySnap, func(events.Event) {}, []Scenario{sc})
	res, _ := r.RunSync(context.Background(), "a")
	if res.Passed || ran || res.Steps[1].Err != "boom" {
		t.Fatalf("%+v ran=%v", res, ran)
	}
}

func TestBaselineFailureFailsScenario(t *testing.T) {
	sc := Scenario{ID: "a", Steps: []Step{Action("x", func(context.Context, *Ctx) error { return nil })}}
	failing := func(context.Context, lab.ResetOpts) error { return errors.New("no brokers") }
	res, _ := newRunner(failing, emptySnap, func(events.Event) {}, []Scenario{sc}).RunSync(context.Background(), "a")
	if res.Passed || len(res.Steps) != 1 {
		t.Fatalf("%+v", res)
	}
}

func TestBusyRejectsRunAndReset(t *testing.T) {
	release := make(chan struct{})
	finished := make(chan struct{}, 2)
	block := Scenario{ID: "a", Steps: []Step{Action("block", func(context.Context, *Ctx) error { <-release; return nil })}}
	quick := Scenario{ID: "b"}
	r := newRunner(noReset, emptySnap, func(e events.Event) {
		if e.Type == "scenario" {
			finished <- struct{}{}
		}
	}, []Scenario{block, quick})
	if err := r.Start("a"); err != nil {
		t.Fatal(err)
	}
	if err := r.Start("b"); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	if err := r.Reset(); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy for reset, got %v", err)
	}
	close(release)
	<-finished
	if err := r.Start("b"); err != nil {
		t.Fatal(err)
	}
	<-finished
}

func TestUnknownScenario(t *testing.T) {
	r := newRunner(noReset, emptySnap, func(events.Event) {}, nil)
	if err := r.Start("nope"); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
}
