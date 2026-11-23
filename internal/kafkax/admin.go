package kafkax

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type PartitionInfo struct {
	Partition int32   `json:"partition"`
	Leader    int32   `json:"leader"`
	Replicas  []int32 `json:"replicas"`
	ISR       []int32 `json:"isr"`
	End       int64   `json:"end"`
}

type PartitionLag struct {
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"` // -1 when the group never committed this partition
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
	OwnerHost string `json:"ownerHost"`
}

type GroupState struct {
	Name     string         `json:"name"`
	State    string         `json:"state"`
	Members  int            `json:"members"`
	Assigned int            `json:"assigned"`
	Lags     []PartitionLag `json:"lags"`
}

type Admin struct {
	cl  *kgo.Client
	adm *kadm.Client
}

// NewAdmin connects lazily, so it succeeds while the cluster is still down.
func NewAdmin() (*Admin, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(Brokers...))
	if err != nil {
		return nil, err
	}
	return &Admin{cl: cl, adm: kadm.NewClient(cl)}, nil
}

func (a *Admin) Close() { a.cl.Close() }

func (a *Admin) BrokerCount(ctx context.Context) (int, error) {
	bs, err := a.adm.ListBrokers(ctx)
	return len(bs), err
}

// RecreateTopics deletes and recreates specs; deletion is asynchronous, so creation retries until it lands.
func (a *Admin) RecreateTopics(ctx context.Context, specs []TopicSpec) error {
	names := make([]string, len(specs))
	for i, sp := range specs {
		names[i] = sp.Name
	}
	resp, err := a.adm.DeleteTopics(ctx, names...)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("delete %s: %w", r.Topic, r.Err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, sp := range specs {
		for {
			cr, err := a.adm.CreateTopics(ctx, sp.Partitions, sp.RF, sp.Configs, sp.Name)
			if err == nil {
				err = cr[sp.Name].Err
			}
			if err == nil {
				break
			}
			if !errors.Is(err, kerr.TopicAlreadyExists) || time.Now().After(deadline) {
				return fmt.Errorf("create %s: %w", sp.Name, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	// A new topic answers NOT_LEADER until every partition's leader is known; callers need it usable.
	for _, sp := range specs {
		for {
			_, err := a.Partitions(ctx, sp.Name)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("topic %s not ready: %w", sp.Name, err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(300 * time.Millisecond):
			}
		}
	}
	return nil
}

func (a *Admin) endOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	lo, err := a.adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends := map[int32]int64{}
	for p, o := range lo[topic] {
		if o.Err != nil {
			return nil, fmt.Errorf("end offset %s-%d: %w", topic, p, o.Err)
		}
		ends[p] = o.Offset
	}
	return ends, nil
}

func (a *Admin) Partitions(ctx context.Context, topic string) ([]PartitionInfo, error) {
	md, err := a.adm.Metadata(ctx, topic)
	if err != nil {
		return nil, err
	}
	td, ok := md.Topics[topic]
	if !ok || td.Err != nil {
		return nil, fmt.Errorf("topic %s: %v", topic, td.Err)
	}
	ends, err := a.endOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := make([]PartitionInfo, 0, len(td.Partitions))
	for _, pd := range td.Partitions {
		out = append(out, PartitionInfo{Partition: pd.Partition, Leader: pd.Leader, Replicas: pd.Replicas, ISR: pd.ISR, End: ends[pd.Partition]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })
	return out, nil
}

func (a *Admin) Group(ctx context.Context, group, topic string) (GroupState, error) {
	gs := GroupState{Name: group}
	dg, err := a.adm.DescribeGroups(ctx, group)
	if err != nil {
		return gs, err
	}
	owners := map[int32]string{}
	if d, ok := dg[group]; ok && d.Err == nil {
		gs.State, gs.Members = d.State, len(d.Members)
		for _, m := range d.Members {
			ca, ok := m.Assigned.AsConsumer()
			if !ok {
				continue
			}
			for _, t := range ca.Topics {
				if t.Topic != topic {
					continue
				}
				for _, p := range t.Partitions {
					owners[p] = strings.TrimPrefix(m.ClientHost, "/")
					gs.Assigned++
				}
			}
		}
	}
	committed, err := a.adm.FetchOffsets(ctx, group)
	if err != nil {
		return gs, err
	}
	ends, err := a.endOffsets(ctx, topic)
	if err != nil {
		return gs, err
	}
	for p, end := range ends {
		c := int64(-1)
		if or, ok := committed.Lookup(topic, p); ok && or.Err == nil {
			c = or.At
		}
		gs.Lags = append(gs.Lags, PartitionLag{Partition: p, Committed: c, End: end, Lag: end - max(c, 0), OwnerHost: owners[p]})
	}
	sort.Slice(gs.Lags, func(i, j int) bool { return gs.Lags[i].Partition < gs.Lags[j].Partition })
	return gs, nil
}

// CommitOffsets only works on a group without live members, like kafka-consumer-groups --reset-offsets.
func (a *Admin) CommitOffsets(ctx context.Context, group, topic string, offsets map[int32]int64) error {
	var os kadm.Offsets
	for p, o := range offsets {
		os.AddOffset(topic, p, o, -1)
	}
	resp, err := a.adm.CommitOffsets(ctx, group, os)
	if err != nil {
		return err
	}
	return resp.Error()
}

func (a *Admin) DeleteGroup(ctx context.Context, group string) error {
	resp, err := a.adm.DeleteGroups(ctx, group)
	if err != nil {
		return err
	}
	if r, ok := resp[group]; ok && r.Err != nil && !errors.Is(r.Err, kerr.GroupIDNotFound) {
		return fmt.Errorf("delete group %s: %w", group, r.Err)
	}
	return nil
}

func (a *Admin) EndOffsetSum(ctx context.Context, topic string) (int64, error) {
	ends, err := a.endOffsets(ctx, topic)
	var sum int64
	for _, e := range ends {
		sum += e
	}
	return sum, err
}

type TopicInfo struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	RF         int    `json:"rf"`
}

// ListTopics lists user topics; internal ones such as __consumer_offsets are left out.
func (a *Admin) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	td, err := a.adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	var out []TopicInfo
	for _, t := range td.Sorted() {
		if t.IsInternal || strings.HasPrefix(t.Topic, "__") {
			continue
		}
		rf := 0
		for _, p := range t.Partitions {
			rf = len(p.Replicas)
			break
		}
		out = append(out, TopicInfo{Name: t.Topic, Partitions: len(t.Partitions), RF: rf})
	}
	return out, nil
}

// CreateTopic creates one topic and waits until every partition has a leader.
func (a *Admin) CreateTopic(ctx context.Context, sp TopicSpec) error {
	cr, err := a.adm.CreateTopics(ctx, sp.Partitions, sp.RF, sp.Configs, sp.Name)
	if err == nil {
		err = cr[sp.Name].Err
	}
	if err != nil {
		return fmt.Errorf("create %s: %w", sp.Name, err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := a.Partitions(ctx, sp.Name); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("topic %s not ready: %w", sp.Name, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	resp, err := a.adm.DeleteTopics(ctx, name)
	if err != nil {
		return err
	}
	if r := resp[name]; r.Err != nil && !errors.Is(r.Err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("delete %s: %w", name, r.Err)
	}
	return nil
}

// StartOffsets returns each partition's log start offset (0 until retention deletes something).
func (a *Admin) StartOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	lo, err := a.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	starts := map[int32]int64{}
	for p, o := range lo[topic] {
		if o.Err != nil {
			return nil, fmt.Errorf("start offset %s-%d: %w", topic, p, o.Err)
		}
		starts[p] = o.Offset
	}
	return starts, nil
}
