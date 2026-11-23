package playground

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/internal/kafkax"
)

type RecordView struct {
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	Key         string `json:"key"`
	Value       string `json:"value"`
	Redelivered bool   `json:"redelivered,omitempty"`
}

type SendView struct {
	ID          int64     `json:"id"`
	Pending     bool      `json:"pending,omitempty"`
	Producer    string    `json:"producer"`
	Acks        string    `json:"acks"`
	BrokersDown int       `json:"brokersDown"`
	Topic       string    `json:"topic"`
	Key         string    `json:"key"`
	Value       string    `json:"value"`
	Partition   int32     `json:"partition"`
	Offset      int64     `json:"offset"`
	Err         string    `json:"err,omitempty"`
	TookMs      int64     `json:"tookMs"`
	At          time.Time `json:"at"`
}

type producer struct {
	name    string
	profile kafkax.ProducerProfile
	cl      *kgo.Client
	mu      sync.Mutex
	sends   *ring[SendView]
}

type ProducerView struct {
	Name      string     `json:"name"`
	JavaProps []string   `json:"javaProps"`
	Sends     []SendView `json:"sends"`
}

func (p *producer) view() ProducerView {
	p.mu.Lock()
	defer p.mu.Unlock()
	return ProducerView{Name: p.name, JavaProps: p.profile.JavaProps(), Sends: p.sends.items()}
}

type consumer struct {
	name, group, topic, reset string
	cl                        *kgo.Client
	tracker                   *deliveryTracker

	pollMu sync.Mutex // one poll at a time: manual polls and the auto loop share the client

	mu           sync.Mutex
	assigned     map[int32]bool
	positions    map[int32]int64
	received     *ring[RecordView]
	polls        int
	redeliveries int
	err          string
	stopAuto     context.CancelFunc
}

type ConsumerView struct {
	Name         string          `json:"name"`
	Group        string          `json:"group"`
	Topic        string          `json:"topic"`
	Reset        string          `json:"reset"`
	Assigned     []int32         `json:"assigned"`
	Positions    map[int32]int64 `json:"positions"`
	Received     []RecordView    `json:"received"`
	Polls        int             `json:"polls"`
	Redeliveries int             `json:"redeliveries"`
	Auto         bool            `json:"auto"`
	Err          string          `json:"err,omitempty"`
}

func newConsumer(name, group, topic, reset string, tracker *deliveryTracker) (*consumer, error) {
	c := &consumer{name: name, group: group, topic: topic, reset: reset, tracker: tracker,
		assigned: map[int32]bool{}, positions: map[int32]int64{}, received: newRing[RecordView](30)}
	start := kgo.NewOffset().AtStart()
	if reset == "latest" {
		start = kgo.NewOffset().AtEnd()
	}
	onAssigned := func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, p := range m[topic] {
			c.assigned[p] = true
		}
	}
	onRevoked := func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, p := range m[topic] {
			delete(c.assigned, p)
			delete(c.positions, p)
		}
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(kafkax.Brokers...),
		kgo.ClientID("pg-"+name),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(start),
		kgo.DisableAutoCommit(),
		// The Java client's default list starts with the range assignor; mirror it so assignments look the same.
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.OnPartitionsAssigned(onAssigned),
		kgo.OnPartitionsRevoked(onRevoked),
		kgo.OnPartitionsLost(onRevoked),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

// poll waits up to wait for at most max records and records where each one came from.
func (c *consumer) poll(ctx context.Context, max int, wait time.Duration) ([]RecordView, error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	pctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	fs := c.cl.PollRecords(pctx, max)
	var out []RecordView
	fs.EachRecord(func(r *kgo.Record) {
		rv := RecordView{Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: excerpt(r.Value)}
		rv.Redelivered = c.tracker.observe(c.group, c.topic, r.Partition, r.Offset)
		out = append(out, rv)
	})
	var pollErr error
	for _, fe := range fs.Errors() {
		if !errors.Is(fe.Err, context.DeadlineExceeded) && !errors.Is(fe.Err, context.Canceled) {
			pollErr = fmt.Errorf("%s-%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.polls++
	for _, rv := range out {
		c.positions[rv.Partition] = rv.Offset + 1
		c.received.push(rv)
		if rv.Redelivered {
			c.redeliveries++
		}
	}
	c.err = ""
	if pollErr != nil {
		c.err = pollErr.Error()
	}
	return out, pollErr
}

func (c *consumer) commit(ctx context.Context) error {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()
	return c.cl.CommitUncommittedOffsets(ctx)
}

// setAuto polls and commits in a loop, like a typical consumer service; onBatch sees each committed batch.
func (c *consumer) setAuto(on bool, onBatch func([]RecordView)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !on {
		if c.stopAuto != nil {
			c.stopAuto()
			c.stopAuto = nil
		}
		return
	}
	if c.stopAuto != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.stopAuto = cancel
	go func() {
		for ctx.Err() == nil {
			recs, _ := c.poll(ctx, 100, time.Second)
			if len(recs) > 0 && c.commit(ctx) == nil && onBatch != nil {
				onBatch(recs)
			}
		}
	}()
}

func (c *consumer) close() {
	c.setAuto(false, nil)
	c.cl.Close() // leaves the group, so the others rebalance at once instead of after session.timeout.ms
}

func (c *consumer) view() ConsumerView {
	c.mu.Lock()
	defer c.mu.Unlock()
	v := ConsumerView{Name: c.name, Group: c.group, Topic: c.topic, Reset: c.reset, Positions: map[int32]int64{},
		Received: c.received.items(), Polls: c.polls, Redeliveries: c.redeliveries, Auto: c.stopAuto != nil, Err: c.err}
	for p := range c.assigned {
		v.Assigned = append(v.Assigned, p)
	}
	sort.Slice(v.Assigned, func(i, j int) bool { return v.Assigned[i] < v.Assigned[j] })
	for p, o := range c.positions {
		v.Positions[p] = o
	}
	return v
}

func excerpt(b []byte) string {
	const max = 80
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}
