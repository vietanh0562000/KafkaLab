// Package lab is the facade scenarios and the web API act through: produce, inject faults, reset, observe.
package lab

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"kafkalab/internal/chaos"
	"kafkalab/internal/events"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/observe"
	"kafkalab/internal/payload"
)

type Admin interface {
	BrokerCount(context.Context) (int, error)
	DeleteGroup(context.Context, string) error
	RecreateTopics(context.Context, []kafkax.TopicSpec) error
	CommitOffsets(context.Context, string, string, map[int32]int64) error
	Group(context.Context, string, string) (kafkax.GroupState, error)
}

type DB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Lab struct {
	Docker *chaos.Docker
	Admin  Admin
	DB     DB
	Hub    *events.Hub
	Snap   func() observe.Snapshot

	send func(context.Context, kafkax.ProducerProfile, []kafkax.Record) ([]kafkax.Result, error)
	now  func() time.Time
	seq  atomic.Int64

	mu       sync.Mutex
	timeline *payload.Timeline
	group    string

	freeMu      sync.Mutex
	freeCancels map[int]context.CancelFunc
	freeNext    int
}

func New(d *chaos.Docker, a Admin, db DB, hub *events.Hub) *Lab {
	l := &Lab{Docker: d, Admin: a, DB: db, Hub: hub, send: sendWithProfile, now: time.Now, group: kafkax.Group,
		freeCancels: map[int]context.CancelFunc{}}
	l.timeline = payload.NewTimeline(payload.TodayBerlinUTC(l.now()))
	l.Snap = func() observe.Snapshot { return observe.Snapshot{} }
	return l
}

func sendWithProfile(ctx context.Context, p kafkax.ProducerProfile, recs []kafkax.Record) ([]kafkax.Result, error) {
	cl, err := kafkax.NewProducer(p)
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	return kafkax.ProduceSync(ctx, cl, recs), nil
}

func (l *Lab) Log(format string, args ...any) {
	l.Hub.Publish(events.Event{Type: "log", Data: fmt.Sprintf(format, args...)})
}

func (l *Lab) Snapshot() observe.Snapshot { return l.Snap() }

func (l *Lab) Group() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.group
}

func (l *Lab) NextSeq() int64 { return l.seq.Add(1) }

// NextPoints hands out (sensor, minute) pairs never used since the last reset, so each becomes a new row.
func (l *Lab) NextPoints(n, sensors int) []payload.Point {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.timeline.Batch(n, sensors)
}

func (l *Lab) NextRecords(n, sensors int) []kafkax.Record {
	points := l.NextPoints(n, sensors)
	recs := make([]kafkax.Record, len(points))
	for i, p := range points {
		recs[i] = kafkax.Record{Key: p.SensorID, Value: payload.ShortTerm(p.SensorID, p.Start, l.NextSeq())}
	}
	return recs
}

func (l *Lab) ProduceRecords(ctx context.Context, p kafkax.ProducerProfile, recs []kafkax.Record) ([]kafkax.Result, error) {
	return l.send(ctx, p, recs)
}

type ProduceReq struct {
	Count       int    `json:"count"`
	Sensors     int    `json:"sensors"`
	RatePerSec  int    `json:"ratePerSec"`
	PoisonEvery int    `json:"poisonEvery"`
	Acks        string `json:"acks"`
	Idempotent  bool   `json:"idempotent"`
}

// Limits keep one request from exhausting memory or making a zero ticker interval.
const (
	maxProduceCount = 1_000_000
	maxProduceRate  = 10_000
	maxSensors      = 100_000
)

func (r ProduceReq) validate() error {
	switch {
	case r.Count < 1 || r.Count > maxProduceCount:
		return fmt.Errorf("count must be 1..%d", maxProduceCount)
	case r.RatePerSec < 0 || r.RatePerSec > maxProduceRate:
		return fmt.Errorf("ratePerSec must be 0..%d", maxProduceRate)
	case r.Sensors < 0 || r.Sensors > maxSensors:
		return fmt.Errorf("sensors must be 0..%d", maxSensors)
	case r.PoisonEvery < 0:
		return fmt.Errorf("poisonEvery must be >= 0")
	}
	return nil
}

// trackFreePlay makes a free-play produce cancellable by Reset, so it cannot leak into the next baseline.
func (l *Lab) trackFreePlay(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	l.freeMu.Lock()
	id := l.freeNext
	l.freeNext++
	l.freeCancels[id] = cancel
	l.freeMu.Unlock()
	return ctx, func() {
		l.freeMu.Lock()
		delete(l.freeCancels, id)
		l.freeMu.Unlock()
		cancel()
	}
}

func (l *Lab) cancelFreePlay() {
	l.freeMu.Lock()
	defer l.freeMu.Unlock()
	for _, cancel := range l.freeCancels {
		cancel()
	}
}

// Produce is free play: Count records over Sensors keys, every PoisonEvery-th one a poison pill.
func (l *Lab) Produce(ctx context.Context, r ProduceReq) ([]kafkax.Result, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	ctx, done := l.trackFreePlay(ctx)
	defer done()
	if r.Acks == "" {
		r.Acks = "all"
	}
	profile := kafkax.ProducerProfile{Acks: r.Acks, Idempotent: r.Idempotent}
	points := l.NextPoints(r.Count, r.Sensors)
	recs := make([]kafkax.Record, len(points))
	for i, p := range points {
		v := payload.ShortTerm(p.SensorID, p.Start, l.NextSeq())
		if r.PoisonEvery > 0 && (i+1)%r.PoisonEvery == 0 {
			v = payload.Poison(p.Start, l.NextSeq())
		}
		recs[i] = kafkax.Record{Key: p.SensorID, Value: v}
	}
	l.Log("produce %d records (%v)", len(recs), profile.JavaProps())
	if r.RatePerSec <= 0 {
		return l.send(ctx, profile, recs)
	}
	var out []kafkax.Result
	tick := time.NewTicker(time.Second / time.Duration(r.RatePerSec))
	defer tick.Stop()
	for _, rec := range recs {
		res, err := l.send(ctx, profile, []kafkax.Record{rec})
		if err != nil {
			return out, err
		}
		out = append(out, res...)
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-tick.C:
		}
	}
	return out, nil
}

// Chaos applies one fault action to a lab container; network actions use the Kafka network only.
func (l *Lab) Chaos(ctx context.Context, action, target string) error {
	d := l.Docker
	switch action {
	case "stop":
		return d.Stop(ctx, target)
	case "start":
		return d.Start(ctx, target)
	case "kill":
		return d.Kill(ctx, target)
	case "pause":
		return d.Pause(ctx, target)
	case "unpause":
		return d.Unpause(ctx, target)
	case "disconnect":
		return d.Disconnect(ctx, "lab-kafka", target)
	case "connect":
		return d.Connect(ctx, "lab-kafka", target)
	}
	return fmt.Errorf("unknown chaos action %q", action)
}

func (l *Lab) RowExists(ctx context.Context, permanentID string, start time.Time) (bool, error) {
	var ok bool
	err := l.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM traffic_data.traffic_data_sensor_short_term_data
		WHERE permanent_id = $1 AND start_time = $2)`, permanentID, start).Scan(&ok)
	return ok, err
}
