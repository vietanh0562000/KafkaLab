// Package playground lets the learner drive real producers and consumers against the lab cluster by hand.
package playground

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kafkalab/internal/kafkax"
)

// Brokers are the lab's broker containers, in node-id order.
var Brokers = []string{"lab-kafka-1", "lab-kafka-2", "lab-kafka-3"}

type Manager struct {
	admin *kafkax.Admin
	// Containers reports docker states; nil means unknown, which counts every broker as up.
	Containers func(ctx context.Context) (map[string]string, error)

	tracker *deliveryTracker

	mu        sync.Mutex
	producers map[string]*producer
	consumers map[string]*consumer
	tails     map[string]*tail
	facts     *facts
	done      map[string]bool
	sendSeq   atomic.Int64

	produce func(ctx context.Context, p *producer, topic string, recs []kafkax.Record) []kafkax.Result
}

func New(a *kafkax.Admin) *Manager {
	return &Manager{admin: a, tracker: newDeliveryTracker(), producers: map[string]*producer{},
		consumers: map[string]*consumer{}, tails: map[string]*tail{}, facts: newFacts(), done: map[string]bool{},
		produce: func(ctx context.Context, p *producer, topic string, recs []kafkax.Record) []kafkax.Result {
			return kafkax.ProduceTo(ctx, p.cl, topic, recs)
		}}
}

func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeClientsLocked()
}

func (m *Manager) closeClientsLocked() {
	for _, c := range m.consumers {
		c.close()
	}
	for _, p := range m.producers {
		p.cl.Close()
	}
	for _, t := range m.tails {
		t.close()
	}
	m.producers, m.consumers, m.tails = map[string]*producer{}, map[string]*consumer{}, map[string]*tail{}
}

func (m *Manager) AddProducer(name string, prof kafkax.ProducerProfile) error {
	if err := validateClientName(name); err != nil {
		return err
	}
	// Short enough to watch a send give up in the UI; the Java default is 120 s.
	if prof.DeliveryTimeout == 0 {
		prof.DeliveryTimeout = 10 * time.Second
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.producers[name]; ok {
		return fmt.Errorf("producer %s already exists", name)
	}
	cl, err := kafkax.NewProducer(prof)
	if err != nil {
		return err
	}
	m.producers[name] = &producer{name: name, profile: prof, cl: cl, sends: newRing[SendView](20)}
	return nil
}

func (m *Manager) RemoveProducer(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.producers[name]
	if !ok {
		return fmt.Errorf("no producer %s", name)
	}
	delete(m.producers, name)
	go p.cl.Close() // waits for in-flight records, which can take the whole delivery timeout
	return nil
}

const maxSendCount = 1000

// Send produces count records; "{i}" in key or value is replaced by 1..count.
func (m *Manager) Send(ctx context.Context, producerName, topic, key, value string, count int) ([]SendView, error) {
	m.mu.Lock()
	p, ok := m.producers[producerName]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no producer %s", producerName)
	}
	if count < 1 || count > maxSendCount {
		return nil, fmt.Errorf("count must be 1..%d", maxSendCount)
	}
	if topic == "" {
		return nil, errors.New("topic is required")
	}
	recs := make([]kafkax.Record, count)
	for i := range recs {
		n := strconv.Itoa(i + 1)
		recs[i] = kafkax.Record{Key: strings.ReplaceAll(key, "{i}", n), Value: []byte(strings.ReplaceAll(value, "{i}", n))}
	}
	down := m.brokersDown(ctx)
	start := time.Now()
	out := make([]SendView, len(recs))
	p.mu.Lock()
	for i := range recs {
		out[i] = SendView{ID: m.sendSeq.Add(1), Pending: true, Producer: producerName, Acks: p.profile.Acks, BrokersDown: down,
			Topic: topic, Key: recs[i].Key, Value: excerpt(recs[i].Value), Partition: -1, Offset: -1, At: start}
		p.sends.push(out[i])
	}
	p.mu.Unlock()
	// An idempotent producer may keep a record in flight past any deadline, so the UI shows it as pending meanwhile.
	res := m.produce(ctx, p, topic, recs)
	took := time.Since(start).Milliseconds()
	p.mu.Lock()
	for i, r := range res {
		out[i].Pending, out[i].TookMs, out[i].Partition, out[i].Offset = false, took, r.Partition, r.Offset
		if r.Err != nil {
			out[i].Err, out[i].Partition, out[i].Offset = r.Err.Error(), -1, -1
		}
		p.sends.replace(func(sv SendView) bool { return sv.ID == out[i].ID }, out[i])
	}
	p.mu.Unlock()
	m.facts.recordSends(out)
	return out, nil
}

func (m *Manager) brokersDown(ctx context.Context) int {
	if m.Containers == nil {
		return 0
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	states, err := m.Containers(cctx)
	if err != nil {
		return 0
	}
	n := 0
	for _, b := range Brokers {
		if states[b] != "running" {
			n++
		}
	}
	return n
}

func (m *Manager) AddConsumer(name, group, topic, reset string) error {
	if err := validateClientName(name); err != nil {
		return err
	}
	if group == "" || topic == "" {
		return errors.New("group and topic are required")
	}
	if reset != "earliest" && reset != "latest" {
		return fmt.Errorf("auto.offset.reset must be earliest or latest, not %q", reset)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.consumers[name]; ok {
		return fmt.Errorf("consumer %s already exists", name)
	}
	c, err := newConsumer(name, group, topic, reset, m.tracker)
	if err != nil {
		return err
	}
	m.consumers[name] = c
	return nil
}

func (m *Manager) consumer(name string) (*consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.consumers[name]
	if !ok {
		return nil, fmt.Errorf("no consumer %s", name)
	}
	return c, nil
}

func (m *Manager) RemoveConsumer(name string) error {
	m.mu.Lock()
	c, ok := m.consumers[name]
	delete(m.consumers, name)
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("no consumer %s", name)
	}
	c.close()
	return nil
}

func (m *Manager) Poll(ctx context.Context, name string, max int) ([]RecordView, error) {
	c, err := m.consumer(name)
	if err != nil {
		return nil, err
	}
	if max < 1 || max > 500 {
		max = 10
	}
	recs, err := c.poll(ctx, max, 3*time.Second)
	m.facts.recordPoll(c.group, c.topic, recs)
	return recs, err
}

func (m *Manager) Commit(ctx context.Context, name string) error {
	c, err := m.consumer(name)
	if err != nil {
		return err
	}
	if err := c.commit(ctx); err != nil {
		return err
	}
	m.facts.set(c.topic, factCommitted)
	return nil
}

func (m *Manager) SetAuto(name string, on bool) error {
	c, err := m.consumer(name)
	if err != nil {
		return err
	}
	c.setAuto(on, func(recs []RecordView) {
		m.facts.recordPoll(c.group, c.topic, recs)
		m.facts.set(c.topic, factCommitted)
	})
	return nil
}

type TopicReq struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	RF         int    `json:"rf"`
	MinISR     int    `json:"minIsr"`
}

func (m *Manager) CreateTopic(ctx context.Context, r TopicReq) error {
	if err := validateTopicName(r.Name); err != nil {
		return err
	}
	if r.Partitions < 1 || r.Partitions > 12 || r.RF < 1 || r.RF > len(Brokers) || r.MinISR < 1 || r.MinISR > r.RF {
		return fmt.Errorf("need 1-12 partitions, RF 1-%d and 1 <= min.insync.replicas <= RF", len(Brokers))
	}
	isr := strconv.Itoa(r.MinISR)
	err := m.admin.CreateTopic(ctx, kafkax.TopicSpec{Name: r.Name, Partitions: int32(r.Partitions), RF: int16(r.RF),
		Configs: map[string]*string{"min.insync.replicas": &isr}})
	m.dropTopicState(r.Name)
	return err
}

func (m *Manager) DeleteTopic(ctx context.Context, name string) error {
	if err := validateTopicName(name); err != nil {
		return err
	}
	err := m.admin.DeleteTopic(ctx, name)
	m.dropTopicState(name)
	return err
}

// dropTopicState forgets what was seen of a topic, so a recreated one does not inherit offsets or facts.
func (m *Manager) dropTopicState(name string) {
	m.mu.Lock()
	if t, ok := m.tails[name]; ok {
		t.close()
		delete(m.tails, name)
	}
	m.mu.Unlock()
	m.tracker.forgetTopic(name)
}

// Clear closes every client and deletes the playground's topics and groups; scenario ones are kept.
func (m *Manager) Clear(ctx context.Context) error {
	m.mu.Lock()
	m.closeClientsLocked()
	m.facts, m.done = newFacts(), map[string]bool{}
	m.mu.Unlock()
	m.tracker.reset()
	topics, err := m.admin.ListTopics(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, t := range topics {
		if validateTopicName(t.Name) == nil {
			errs = append(errs, m.admin.DeleteTopic(ctx, t.Name))
		}
	}
	groups, err := m.admin.ListGroups(ctx)
	if err != nil {
		return err
	}
	for _, g := range groups {
		if !strings.HasPrefix(g, kafkax.Group) {
			errs = append(errs, m.admin.DeleteGroup(ctx, g))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) tail(topic string) (*tail, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tails[topic]; ok {
		return t, nil
	}
	t, err := newTail(topic)
	if err != nil {
		return nil, err
	}
	m.tails[topic] = t
	return t, nil
}

func (m *Manager) clients() ([]*producer, []*consumer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := make([]*producer, 0, len(m.producers))
	for _, p := range m.producers {
		ps = append(ps, p)
	}
	cs := make([]*consumer, 0, len(m.consumers))
	for _, c := range m.consumers {
		cs = append(cs, c)
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].name < ps[j].name })
	sort.Slice(cs, func(i, j int) bool { return cs[i].name < cs[j].name })
	return ps, cs
}
