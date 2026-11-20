package lab

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"kafkalab/internal/chaos"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/payload"
)

type ResetOpts struct {
	Instances int
	Env       chaos.SvcEnv
}

var baseServices = []string{"controller", "kafka-1", "kafka-2", "kafka-3", "kafka-ui", "timescaledb"}

func svcNames(from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprintf("gap-svc-%d", i))
	}
	return out
}

func waitFor(ctx context.Context, timeout time.Duration, what string, ok func(context.Context) (bool, error)) error {
	deadline := time.Now().Add(timeout)
	var last error
	for {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		done, err := ok(c)
		cancel()
		if done {
			return nil
		}
		last = err
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s waiting for %s (last error: %v)", timeout, what, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Reset returns the lab to a known baseline whatever the last scenario broke.
func (l *Lab) Reset(ctx context.Context, o ResetOpts) error {
	if o.Env.GroupID == "" {
		o.Env.GroupID = kafkax.Group
	}
	l.cancelFreePlay()
	states, err := l.Docker.States(ctx)
	if err != nil {
		return err
	}
	for name, state := range states {
		if state == "paused" {
			if err := l.Docker.Unpause(ctx, name); err != nil {
				return err
			}
		}
	}
	l.Log("reset: starting brokers, controller and DB")
	if err := l.Docker.ComposeUp(ctx, baseServices...); err != nil {
		return err
	}
	if err := l.Docker.StopSvc(ctx, svcNames(1, chaos.MaxSvc)...); err != nil {
		return err
	}
	if err := waitFor(ctx, 60*time.Second, "3 brokers", func(c context.Context) (bool, error) {
		n, err := l.Admin.BrokerCount(c)
		return n == 3, err
	}); err != nil {
		return err
	}
	if err := waitFor(ctx, 60*time.Second, "the database", func(c context.Context) (bool, error) {
		var one int
		err := l.DB.QueryRow(c, "SELECT 1").Scan(&one)
		return err == nil, err
	}); err != nil {
		return err
	}

	old := l.Group()
	for _, g := range []string{old, old + "-tracking", o.Env.GroupID, o.Env.GroupID + "-tracking"} {
		// A fresh cluster answers COORDINATOR_NOT_AVAILABLE until __consumer_offsets exists.
		if err := waitFor(ctx, 60*time.Second, "deleting group "+g, func(c context.Context) (bool, error) {
			err := l.Admin.DeleteGroup(c, g)
			return err == nil, err
		}); err != nil {
			return err
		}
	}
	l.Log("reset: recreating topics")
	if err := l.Admin.RecreateTopics(ctx, kafkax.LabTopics()); err != nil {
		return err
	}
	// Without a committed offset, auto-offset-reset=latest would skip records produced before the
	// consumers finish joining. A scenario that changes the group wants exactly that, so it is left alone.
	if o.Env.GroupID == kafkax.Group {
		zero := map[int32]int64{0: 0, 1: 0, 2: 0}
		for _, g := range []string{kafkax.Group, kafkax.TrackingGroup} {
			if err := waitFor(ctx, 60*time.Second, "committing offset 0 for "+g, func(c context.Context) (bool, error) {
				err := l.Admin.CommitOffsets(c, g, kafkax.Topic, zero)
				return err == nil, err
			}); err != nil {
				return err
			}
		}
	}
	_, err = l.DB.Exec(ctx, "TRUNCATE traffic_data.traffic_data_sensor_short_term_data, traffic_data.late_arrival_tracking")
	var pgErr *pgconn.PgError
	if err != nil && !(errors.As(err, &pgErr) && pgErr.Code == "42P01") {
		return err
	}

	l.Log("reset: starting %d service instance(s), group %s", o.Instances, o.Env.GroupID)
	if err := l.Docker.RecreateSvc(ctx, o.Env, o.Instances); err != nil {
		return err
	}
	l.mu.Lock()
	l.group = o.Env.GroupID
	l.timeline = payload.NewTimeline(payload.TodayBerlinUTC(l.now()))
	l.mu.Unlock()
	if o.Instances > 0 {
		for _, g := range []string{o.Env.GroupID, o.Env.GroupID + "-tracking"} {
			if err := waitFor(ctx, 120*time.Second, "group "+g+" to own all partitions", func(c context.Context) (bool, error) {
				gs, err := l.Admin.Group(c, g, kafkax.Topic)
				return gs.State == "Stable" && gs.Assigned == 3, err
			}); err != nil {
				return err
			}
		}
	}
	l.Log("reset done")
	return nil
}
