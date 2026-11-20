package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"kafkalab/internal/chaos"
	"kafkalab/internal/events"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/payload"
)

type recorder struct{ log []string }

func (r *recorder) add(format string, args ...any) {
	r.log = append(r.log, fmt.Sprintf(format, args...))
}

func (r *recorder) index(prefix string) int {
	for i, l := range r.log {
		if strings.HasPrefix(l, prefix) {
			return i
		}
	}
	return -1
}

type fakeRunner struct {
	rec *recorder
	ps  string
}

func (f *fakeRunner) Run(_ context.Context, _ string, args ...string) (string, error) {
	switch args[0] {
	case "inspect":
		return "kafkalab\n", nil
	case "ps":
		return f.ps, nil
	case "compose":
		f.rec.add("compose %s", strings.Join(args[7:], " ")) // drop -p/-f/--project-directory
	default:
		f.rec.add("docker %s", strings.Join(args, " "))
	}
	return "", nil
}

type fakeAdmin struct {
	rec            *recorder
	coordinatorErr int // DeleteGroup fails this many times first, like a fresh cluster without __consumer_offsets
}

func (a *fakeAdmin) BrokerCount(context.Context) (int, error) { return 3, nil }
func (a *fakeAdmin) DeleteGroup(_ context.Context, g string) error {
	if a.coordinatorErr > 0 {
		a.coordinatorErr--
		return errors.New("COORDINATOR_NOT_AVAILABLE")
	}
	a.rec.add("deleteGroup %s", g)
	return nil
}
func (a *fakeAdmin) RecreateTopics(_ context.Context, specs []kafkax.TopicSpec) error {
	a.rec.add("recreateTopics %d", len(specs))
	return nil
}
func (a *fakeAdmin) CommitOffsets(_ context.Context, g, topic string, off map[int32]int64) error {
	a.rec.add("commit %s %s %v", g, topic, off)
	return nil
}
func (a *fakeAdmin) Group(_ context.Context, g, _ string) (kafkax.GroupState, error) {
	return kafkax.GroupState{Name: g, State: "Stable", Assigned: 3}, nil
}

type fakeRow struct{}

func (fakeRow) Scan(dest ...any) error {
	if p, ok := dest[0].(*int); ok {
		*p = 1
	}
	return nil
}

type fakeDB struct{ rec *recorder }

func (d *fakeDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	d.rec.add("exec %s", sql)
	return pgconn.CommandTag{}, nil
}
func (d *fakeDB) QueryRow(context.Context, string, ...any) pgx.Row { return fakeRow{} }

func newTestLab(t *testing.T, ps string) (*Lab, *recorder, string) {
	rec := &recorder{}
	dir := t.TempDir()
	d := &chaos.Docker{R: &fakeRunner{rec: rec, ps: ps}, Dir: dir, Project: "kafkalab"}
	return New(d, &fakeAdmin{rec: rec}, &fakeDB{rec: rec}, events.NewHub()), rec, dir
}

func TestResetRestoresBrokenLab(t *testing.T) {
	l, rec, dir := newTestLab(t, "lab-kafka-2\tpaused\nlab-timescaledb\texited\nlab-gap-svc-1\trunning\n")
	if err := l.Reset(context.Background(), ResetOpts{Instances: 2, Env: chaos.DefaultSvcEnv()}); err != nil {
		t.Fatal(err)
	}
	order := []string{
		"docker unpause -- lab-kafka-2",
		"compose up -d controller kafka-1 kafka-2 kafka-3 kafka-ui timescaledb",
		"compose stop gap-svc-1 gap-svc-2 gap-svc-3 gap-svc-4",
		"deleteGroup gap-lab",
		"recreateTopics 3",
		"commit gap-lab lab.short-term map[0:0 1:0 2:0]",
		"commit gap-lab-tracking lab.short-term map[0:0 1:0 2:0]",
		"exec TRUNCATE traffic_data.traffic_data_sensor_short_term_data, traffic_data.late_arrival_tracking",
		"compose up -d --force-recreate gap-svc-1 gap-svc-2",
	}
	last := -1
	for _, want := range order {
		i := rec.index(want)
		if i <= last {
			t.Fatalf("%q missing or out of order in %v", want, rec.log)
		}
		last = i
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	if !strings.Contains(string(env), "SVC_DLQ_ENABLED=false") {
		t.Fatalf("env %q", env)
	}
}

func TestResetWithNewGroupSkipsCommit(t *testing.T) {
	l, rec, _ := newTestLab(t, "")
	env := chaos.SvcEnv{GroupID: "gap-lab-new"}
	if err := l.Reset(context.Background(), ResetOpts{Instances: 1, Env: env}); err != nil {
		t.Fatal(err)
	}
	if rec.index("commit gap-lab-new") >= 0 {
		t.Fatalf("new group must start without committed offsets: %v", rec.log)
	}
	if l.Group() != "gap-lab-new" {
		t.Fatal(l.Group())
	}
}

func TestProducePoisonEvery(t *testing.T) {
	l, _, _ := newTestLab(t, "")
	var sent []kafkax.Record
	l.send = func(_ context.Context, _ kafkax.ProducerProfile, recs []kafkax.Record) ([]kafkax.Result, error) {
		sent = append(sent, recs...)
		return make([]kafkax.Result, len(recs)), nil
	}
	if _, err := l.Produce(context.Background(), ProduceReq{Count: 10, Sensors: 3, PoisonEvery: 5, Acks: "all", Idempotent: true}); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 10 {
		t.Fatalf("sent %d", len(sent))
	}
	poison := string(payload.Poison(payload.TodayBerlinUTC(l.now()), 0))
	for i, r := range sent {
		isPoison := strings.Contains(string(r.Value), strings.Repeat("p", 300))
		if want := i == 4 || i == 9; isPoison != want {
			t.Fatalf("record %d poison=%v (sample %q)", i, isPoison, poison[:40])
		}
	}
}

func TestResetWaitsForGroupCoordinator(t *testing.T) {
	rec := &recorder{}
	d := &chaos.Docker{R: &fakeRunner{rec: rec}, Dir: t.TempDir(), Project: "kafkalab"}
	l := New(d, &fakeAdmin{rec: rec, coordinatorErr: 2}, &fakeDB{rec: rec}, events.NewHub())
	if err := l.Reset(context.Background(), ResetOpts{Instances: 1, Env: chaos.DefaultSvcEnv()}); err != nil {
		t.Fatalf("reset must retry while the coordinator is not available: %v", err)
	}
	if rec.index("deleteGroup gap-lab") < 0 {
		t.Fatal(rec.log)
	}
}

func TestProduceRejectsOutOfRange(t *testing.T) {
	l, _, _ := newTestLab(t, "")
	l.send = func(context.Context, kafkax.ProducerProfile, []kafkax.Record) ([]kafkax.Result, error) {
		return nil, nil
	}
	for _, r := range []ProduceReq{
		{Count: 2, RatePerSec: 2_000_000_000},
		{Count: 2_000_000_000},
		{Count: 10, Sensors: -1},
		{Count: 10, PoisonEvery: -1},
	} {
		if _, err := l.Produce(context.Background(), r); err == nil {
			t.Fatalf("want error for %+v", r)
		}
	}
}

func TestResetCancelsFreePlayProduce(t *testing.T) {
	l, _, _ := newTestLab(t, "")
	l.send = func(context.Context, kafkax.ProducerProfile, []kafkax.Record) ([]kafkax.Result, error) {
		return []kafkax.Result{{}}, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := l.Produce(context.Background(), ProduceReq{Count: 1000, RatePerSec: 2})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := l.Reset(context.Background(), ResetOpts{Instances: 1, Env: chaos.DefaultSvcEnv()}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("free-play produce kept running through reset")
	}
}
