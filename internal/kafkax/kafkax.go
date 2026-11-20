// Package kafkax wraps franz-go with the lab's topics, producer profiles and admin queries.
package kafkax

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	Topic          = "lab.short-term"
	DLQTopic       = "dlq-lab.short-term"
	CompactedTopic = "lab.compacted"
	Group          = "gap-lab"
	// TrackingGroup is the service's second consumer group on the same topic (late-arrival tracking).
	TrackingGroup = "gap-lab-tracking"
)

var Brokers = []string{"localhost:19092", "localhost:19093", "localhost:19094"}

// ProducerProfile names producer settings the way the Java client does, so the UI can show them verbatim.
type ProducerProfile struct {
	Acks            string // "all", "1" or "0"
	Idempotent      bool
	TxnID           string
	RequestTimeout  time.Duration
	MaxRequestBytes int32
}

func DefaultProfile() ProducerProfile { return ProducerProfile{Acks: "all", Idempotent: true} }

func (p ProducerProfile) JavaProps() []string {
	props := []string{"acks=" + p.Acks, fmt.Sprintf("enable.idempotence=%t", p.Idempotent)}
	if p.TxnID != "" {
		props = append(props, "transactional.id="+p.TxnID)
	}
	if p.RequestTimeout > 0 {
		props = append(props, fmt.Sprintf("request.timeout.ms=%d", p.RequestTimeout.Milliseconds()))
	}
	if p.MaxRequestBytes > 0 {
		props = append(props, fmt.Sprintf("max.request.size=%d", p.MaxRequestBytes))
	}
	return props
}

func NewProducer(p ProducerProfile) (*kgo.Client, error) {
	// The Java client refuses the same combination: idempotence needs every in-sync replica to ack.
	if p.Idempotent && p.Acks != "all" {
		return nil, errors.New("enable.idempotence=true requires acks=all")
	}
	// franz-go retries forever by default; a bound keeps a dead cluster from hanging a scenario.
	opts := []kgo.Opt{kgo.SeedBrokers(Brokers...), kgo.RecordDeliveryTimeout(30 * time.Second)}
	switch p.Acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "0":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		return nil, fmt.Errorf("unknown acks %q", p.Acks)
	}
	if !p.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	if p.TxnID != "" {
		opts = append(opts, kgo.TransactionalID(p.TxnID))
	}
	if p.RequestTimeout > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(p.RequestTimeout))
	}
	if p.MaxRequestBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(p.MaxRequestBytes), kgo.BrokerMaxWriteBytes(p.MaxRequestBytes+1<<20))
	}
	return kgo.NewClient(opts...)
}

type Record struct {
	Key   string
	Value []byte
}

type Result struct {
	Partition int32
	Offset    int64
	Err       error
}

// ProduceSync sends recs and returns one result per record, in order.
func ProduceSync(ctx context.Context, cl *kgo.Client, recs []Record) []Result {
	krs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		krs[i] = &kgo.Record{Topic: Topic, Key: []byte(r.Key), Value: r.Value}
		if r.Key == "" {
			krs[i].Key = nil
		}
	}
	res := cl.ProduceSync(ctx, krs...)
	out := make([]Result, len(res))
	for i, r := range res {
		out[i] = Result{Partition: r.Record.Partition, Offset: r.Record.Offset, Err: r.Err}
	}
	return out
}

type TopicSpec struct {
	Name       string
	Partitions int32
	RF         int16
	Configs    map[string]*string
}

func s(v string) *string { return &v }

func LabTopics() []TopicSpec {
	return []TopicSpec{
		{Name: Topic, Partitions: 3, RF: 3, Configs: map[string]*string{
			"min.insync.replicas": s("2"), "segment.ms": s("10000")}},
		{Name: DLQTopic, Partitions: 3, RF: 3, Configs: map[string]*string{"min.insync.replicas": s("2")}},
		{Name: CompactedTopic, Partitions: 3, RF: 3, Configs: map[string]*string{
			"cleanup.policy": s("compact"), "segment.ms": s("10000"),
			"min.cleanable.dirty.ratio": s("0.01"), "delete.retention.ms": s("10000")}},
	}
}
