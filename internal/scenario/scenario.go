// Package scenario runs guided experiments: a baseline, then steps that act, wait and assert.
package scenario

import (
	"context"
	"fmt"
	"time"

	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
)

// Ctx is what steps share during one run; Vars carries values from one step to a later one.
type Ctx struct {
	Lab      *lab.Lab
	Snapshot func() observe.Snapshot
	Vars     map[string]any
}

type Step struct {
	Name string
	Do   func(context.Context, *Ctx) (observed string, err error)
}

func Action(name string, fn func(context.Context, *Ctx) error) Step {
	return Step{Name: name, Do: func(ctx context.Context, c *Ctx) (string, error) { return "", fn(ctx, c) }}
}

// ActionObserved is an Action that reports what it measured.
func ActionObserved(name string, fn func(context.Context, *Ctx) (string, error)) Step {
	return Step{Name: name, Do: fn}
}

func WaitUntil(name string, timeout time.Duration, pred func(observe.Snapshot, *Ctx) bool) Step {
	return Step{Name: name, Do: func(ctx context.Context, c *Ctx) (string, error) {
		start := time.Now()
		for {
			if pred(c.Snapshot(), c) {
				return fmt.Sprintf("after %.1fs", time.Since(start).Seconds()), nil
			}
			if time.Since(start) > timeout {
				return fmt.Sprintf("timeout after %s", timeout), fmt.Errorf("condition not met within %s", timeout)
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}}
}

func Expect(name string, check func(observe.Snapshot, *Ctx) (bool, string)) Step {
	return Step{Name: name, Do: func(_ context.Context, c *Ctx) (string, error) {
		ok, observed := check(c.Snapshot(), c)
		if !ok {
			return observed, fmt.Errorf("expectation not met")
		}
		return observed, nil
	}}
}

type Scenario struct {
	ID, Module, Title string
	Question          string
	Reveal            string // HTML
	Baseline          lab.ResetOpts
	Steps             []Step
	JavaProps         []string
	CLI               []string
	Confirm           []string
}

type Summary struct {
	ID        string   `json:"id"`
	Module    string   `json:"module"`
	Title     string   `json:"title"`
	Question  string   `json:"question"`
	Reveal    string   `json:"reveal"`
	JavaProps []string `json:"javaProps"`
	CLI       []string `json:"cli"`
	Confirm   []string `json:"confirm"`
}

func (s Scenario) Summary() Summary {
	return Summary{ID: s.ID, Module: s.Module, Title: s.Title, Question: s.Question, Reveal: s.Reveal,
		JavaProps: s.JavaProps, CLI: s.CLI, Confirm: s.Confirm}
}

// All lists every built scenario in learning order.
func All() []Scenario {
	return m0()
}
