package scenario

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"kafkalab/internal/chaos"
	"kafkalab/internal/events"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
)

var (
	ErrBusy    = errors.New("another scenario or reset is running")
	ErrUnknown = errors.New("unknown scenario")
)

type StepEvent struct {
	Scenario string `json:"scenario"`
	Step     string `json:"step"`
	Status   string `json:"status"` // running | pass | fail
	Observed string `json:"observed,omitempty"`
	Err      string `json:"err,omitempty"`
}

type StepResult struct {
	Name     string        `json:"name"`
	Passed   bool          `json:"passed"`
	Observed string        `json:"observed,omitempty"`
	Err      string        `json:"err,omitempty"`
	Took     time.Duration `json:"took"`
}

type Result struct {
	ID     string       `json:"id"`
	Passed bool         `json:"passed"`
	Steps  []StepResult `json:"steps"`
}

const runTimeout = 20 * time.Minute

type Runner struct {
	lab   *lab.Lab
	reset func(context.Context, lab.ResetOpts) error
	snap  func() observe.Snapshot
	emit  func(events.Event)
	order []Scenario
	byID  map[string]Scenario

	mu   sync.Mutex
	busy bool
}

func NewRunner(l *lab.Lab, hub *events.Hub, all []Scenario) *Runner {
	r := newRunner(l.Reset, l.Snapshot, hub.Publish, all)
	r.lab = l
	return r
}

func newRunner(reset func(context.Context, lab.ResetOpts) error, snap func() observe.Snapshot,
	emit func(events.Event), all []Scenario) *Runner {
	r := &Runner{reset: reset, snap: snap, emit: emit, order: all, byID: map[string]Scenario{}}
	for _, s := range all {
		r.byID[s.ID] = s
	}
	return r
}

func (r *Runner) Summaries() []Summary {
	out := make([]Summary, len(r.order))
	for i, s := range r.order {
		out[i] = s.Summary()
	}
	return out
}

func (r *Runner) acquire() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.busy {
		return ErrBusy
	}
	r.busy = true
	return nil
}

func (r *Runner) release() {
	r.mu.Lock()
	r.busy = false
	r.mu.Unlock()
}

// Start runs a scenario in the background; its end is announced by a "scenario" event.
func (r *Runner) Start(id string) error {
	sc, ok := r.byID[id]
	if !ok {
		return ErrUnknown
	}
	if err := r.acquire(); err != nil {
		return err
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
		defer cancel()
		res := r.run(ctx, sc)
		r.release() // before announcing, so a client reacting to the event can start the next run
		r.emit(events.Event{Type: "scenario", Data: res})
	}()
	return nil
}

func (r *Runner) RunSync(ctx context.Context, id string) (Result, error) {
	sc, ok := r.byID[id]
	if !ok {
		return Result{}, ErrUnknown
	}
	if err := r.acquire(); err != nil {
		return Result{}, err
	}
	res := r.run(ctx, sc)
	r.release()
	r.emit(events.Event{Type: "scenario", Data: res})
	return res, nil
}

// Reset restores the default baseline (2 instances) under the same busy guard as scenarios.
func (r *Runner) Reset() error {
	if err := r.acquire(); err != nil {
		return err
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), runTimeout)
		defer cancel()
		res := Result{ID: "reset", Passed: true}
		if err := r.reset(ctx, lab.ResetOpts{Instances: 2, Env: chaos.DefaultSvcEnv()}); err != nil {
			res.Passed = false
			res.Steps = []StepResult{{Name: "Reset", Err: err.Error()}}
		}
		r.release()
		r.emit(events.Event{Type: "scenario", Data: res})
	}()
	return nil
}

// do turns a panicking step into a failed one; a crash would leave the injected fault in place with no UI.
func do(ctx context.Context, st Step, c *Ctx) (observed string, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("step panicked: %v", p)
		}
	}()
	return st.Do(ctx, c)
}

func (r *Runner) run(ctx context.Context, sc Scenario) Result {
	c := &Ctx{Lab: r.lab, Snapshot: r.snap, Vars: map[string]any{}}
	baseline := Step{Name: "Baseline", Do: func(ctx context.Context, _ *Ctx) (string, error) {
		return "", r.reset(ctx, sc.Baseline)
	}}
	res := Result{ID: sc.ID, Passed: true}
	for _, st := range append([]Step{baseline}, sc.Steps...) {
		r.emit(events.Event{Type: "step", Data: StepEvent{Scenario: sc.ID, Step: st.Name, Status: "running"}})
		start := time.Now()
		observed, err := do(ctx, st, c)
		sr := StepResult{Name: st.Name, Passed: err == nil, Observed: observed, Took: time.Since(start)}
		ev := StepEvent{Scenario: sc.ID, Step: st.Name, Status: "pass", Observed: observed}
		if err != nil {
			sr.Err, ev.Status, ev.Err = err.Error(), "fail", err.Error()
		}
		res.Steps = append(res.Steps, sr)
		r.emit(events.Event{Type: "step", Data: ev})
		if err != nil {
			res.Passed = false
			break
		}
	}
	return res
}
