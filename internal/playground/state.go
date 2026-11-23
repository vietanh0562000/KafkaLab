package playground

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"kafkalab/internal/kafkax"
)

type BrokerView struct {
	ID         int32  `json:"id"`
	Container  string `json:"container"`
	State      string `json:"state"`      // docker state; "" when unknown
	Registered bool   `json:"registered"` // still known to the controller
}

type PartitionView struct {
	Partition int32        `json:"partition"`
	Leader    int32        `json:"leader"` // -1 while the partition has no leader
	Replicas  []int32      `json:"replicas"`
	ISR       []int32      `json:"isr"`
	Start     int64        `json:"start"`
	End       int64        `json:"end"`
	Records   []RecordView `json:"records"`
}

type TopicView struct {
	Name       string          `json:"name"`
	MinISR     int             `json:"minIsr"`
	Partitions []PartitionView `json:"partitions"`
}

type GroupView struct {
	Name      string          `json:"name"`
	State     string          `json:"state"`
	Members   int             `json:"members"`
	Committed map[int32]int64 `json:"committed"`
}

type State struct {
	At        time.Time          `json:"at"`
	Brokers   []BrokerView       `json:"brokers"`
	Topics    []kafkax.TopicInfo `json:"topics"`
	Topic     *TopicView         `json:"topic"`
	Groups    []GroupView        `json:"groups"`
	Producers []ProducerView     `json:"producers"`
	Consumers []ConsumerView     `json:"consumers"`
	Lessons   []LessonStatus     `json:"lessons"`
	Errors    map[string]string  `json:"errors"`
}

// State reads the cluster as it is now; each source has its own deadline so a dead broker slows nothing else.
func (m *Manager) State(ctx context.Context, topic string) State {
	st := State{At: time.Now(), Errors: map[string]string{}}
	var mu sync.Mutex
	fail := func(src string, err error) {
		mu.Lock()
		st.Errors[src] = err.Error()
		mu.Unlock()
	}
	var lessonTopic *TopicView
	var wg sync.WaitGroup
	section := func(src string, f func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sctx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)
			defer cancel()
			if err := f(sctx); err != nil {
				fail(src, err)
			}
		}()
	}
	section("brokers", func(ctx context.Context) error {
		bs, err := m.brokers(ctx)
		st.Brokers = bs
		return err
	})
	section("topics", func(ctx context.Context) error {
		ts, err := m.admin.ListTopics(ctx)
		st.Topics = ts
		return err
	})
	if topic != "" {
		section("topic", func(ctx context.Context) error {
			tv, err := m.topicView(ctx, topic, true)
			st.Topic = tv
			return err
		})
		section("groups", func(ctx context.Context) error {
			gs, err := m.groups(ctx, topic)
			st.Groups = gs
			return err
		})
	}
	if topic != LessonTopic {
		section("lessonTopic", func(ctx context.Context) error {
			tv, _ := m.topicView(ctx, LessonTopic, false) // absent until lesson 1 creates it
			lessonTopic = tv
			return nil
		})
	}
	ps, cs := m.clients()
	for _, p := range ps {
		st.Producers = append(st.Producers, p.view())
	}
	for _, c := range cs {
		st.Consumers = append(st.Consumers, c.view())
	}
	wg.Wait()
	if topic == LessonTopic {
		lessonTopic = st.Topic
	}
	for _, tv := range []*TopicView{st.Topic, lessonTopic} {
		if tv != nil && isrShrunk(tv) {
			m.facts.set(tv.Name, factIsrShrunk)
		}
	}
	st.Lessons = m.checkLessons(LessonInput{Topic: lessonTopic, Brokers: st.Brokers, Consumers: st.Consumers,
		Facts: m.facts.snapshot()})
	return st
}

func isrShrunk(tv *TopicView) bool {
	for _, p := range tv.Partitions {
		if len(p.ISR) < len(p.Replicas) {
			return true
		}
	}
	return false
}

func (m *Manager) brokers(ctx context.Context) ([]BrokerView, error) {
	out := make([]BrokerView, len(Brokers))
	for i, c := range Brokers {
		out[i] = BrokerView{ID: int32(i + 1), Container: c}
	}
	var firstErr error
	if m.Containers != nil {
		states, err := m.Containers(ctx)
		if err != nil {
			firstErr = err
		}
		for i := range out {
			out[i].State = states[out[i].Container]
		}
	}
	ids, err := m.admin.BrokerIDs(ctx)
	if err != nil && firstErr == nil {
		firstErr = err
	}
	for i := range out {
		out[i].Registered = slices.Contains(ids, out[i].ID)
	}
	return out, firstErr
}

// topicView never fails because one leader is down: leadership comes from metadata, and offsets the dead
// leader cannot answer stay -1 while the last records seen are kept.
func (m *Manager) topicView(ctx context.Context, topic string, withRecords bool) (*TopicView, error) {
	parts, err := m.admin.Leaders(ctx, topic)
	if err != nil {
		return nil, err
	}
	tv := &TopicView{Name: topic}
	var offErr error
	starts, ends := map[int32]int64{}, map[int32]int64{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		octx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		starts, ends, offErr = m.admin.OffsetsPartial(octx, topic)
	}()
	go func() {
		defer wg.Done()
		if v, err := m.admin.TopicConfig(ctx, topic, "min.insync.replicas"); err == nil {
			tv.MinISR, _ = strconv.Atoi(v)
		}
	}()
	wg.Wait()
	var t *tail
	if withRecords {
		if t, err = m.tail(topic); err != nil {
			return nil, err
		}
	}
	for _, p := range parts {
		pv := PartitionView{Partition: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR, Start: -1, End: -1}
		if v, ok := starts[p.Partition]; ok {
			pv.Start = v
		}
		if v, ok := ends[p.Partition]; ok {
			pv.End = v
		}
		if t != nil {
			pv.Records = trimTail(t.records(p.Partition), pv.Start, pv.End)
		}
		tv.Partitions = append(tv.Partitions, pv)
	}
	if offErr != nil {
		return tv, fmt.Errorf("offsets: %w", offErr)
	}
	return tv, nil
}

// groups lists every group that has members on topic or has committed an offset of it.
func (m *Manager) groups(ctx context.Context, topic string) ([]GroupView, error) {
	names, err := m.admin.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	slices.Sort(names)
	var out []GroupView
	for _, g := range names {
		gs, err := m.admin.Group(ctx, g, topic)
		if err != nil {
			return out, err
		}
		gv := GroupView{Name: g, State: gs.State, Members: gs.Members, Committed: map[int32]int64{}}
		for _, l := range gs.Lags {
			if l.Committed >= 0 {
				gv.Committed[l.Partition] = l.Committed
			}
		}
		if gs.Assigned > 0 || len(gv.Committed) > 0 {
			out = append(out, gv)
		}
	}
	return out, nil
}
