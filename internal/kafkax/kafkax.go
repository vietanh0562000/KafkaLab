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
	Compression     string // "" means none, the Java default; franz-go would otherwise default to snappy
	DeliveryTimeout time.Duration
	RequestTimeout  time.Duration
	MaxRequestBytes int32
}

func DefaultProfile() ProducerProfile { return ProducerProfile{Acks: "all", Idempotent: true} }

func (p ProducerProfile) JavaProps() []string {
	props := []string{"acks=" + p.Acks, fmt.Sprintf("enable.idempotence=%t", p.Idempotent), "compression.type=" + p.codec()}
	if p.TxnID != "" {
		props = append(props, "transactional.id="+p.TxnID)
	}
	if p.DeliveryTimeout > 0 {
		props = append(props, fmt.Sprintf("delivery.timeout.ms=%d", p.DeliveryTimeout.Milliseconds()))
	}
	if p.RequestTimeout > 0 {
		props = append(props, fmt.Sprintf("request.timeout.ms=%d", p.RequestTimeout.Milliseconds()))
	}
	if p.MaxRequestBytes > 0 {
		props = append(props, fmt.Sprintf("max.request.size=%d", p.MaxRequestBytes))
	}
	return props
}

func (p ProducerProfile) codec() string {
	if p.Compression == "" {
		return "none"
	}
	return p.Compression
}

func NewProducer(p ProducerProfile) (*kgo.Client, error) {
	// The Java client refuses the same combination: idempotence needs every in-sync replica to ack.
	if p.Idempotent && p.Acks != "all" {
		return nil, errors.New("enable.idempotence=true requires acks=all")
	}
	// franz-go retries forever by default; a bound keeps a dead cluster from hanging a scenario.
	delivery := 30 * time.Second
	if p.DeliveryTimeout > 0 {
		delivery = p.DeliveryTimeout
	}
	opts := []kgo.Opt{kgo.SeedBrokers(Brokers...), kgo.RecordDeliveryTimeout(delivery)}
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
	// The service runs on Alpine, where snappy-java cannot load its glibc-linked native library.
	codecs := map[string]kgo.CompressionCodec{"none": kgo.NoCompression(), "snappy": kgo.SnappyCompression(),
		"lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression(), "gzip": kgo.GzipCompression()}
	codec, ok := codecs[p.codec()]
	if !ok {
		return nil, fmt.Errorf("unknown compression.type %q", p.Compression)
	}
	opts = append(opts, kgo.ProducerBatchCompression(codec))
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

// ProduceSync sends recs to the lab topic and returns one result per record, in order.
func ProduceSync(ctx context.Context, cl *kgo.Client, recs []Record) []Result {
	return ProduceTo(ctx, cl, Topic, recs)
}

// ProduceTo sends recs to topic; an empty key is sent as no key, which leaves the choice to the partitioner.
func ProduceTo(ctx context.Context, cl *kgo.Client, topic string, recs []Record) []Result {
	krs := make([]*kgo.Record, len(recs))
	index := make(map[*kgo.Record]int, len(recs))
	for i, r := range recs {
		krs[i] = &kgo.Record{Topic: topic, Value: r.Value}
		if r.Key != "" {
			krs[i].Key = []byte(r.Key) // a nil key, not an empty one, is what tells the partitioner "no key"
		}
		index[krs[i]] = i
	}
	// Results arrive in completion order, which differs from input order once records span partitions.
	res := cl.ProduceSync(ctx, krs...)
	out := make([]Result, len(recs))
	for _, r := range res {
		out[index[r.Record]] = Result{Partition: r.Record.Partition, Offset: r.Record.Offset, Err: r.Err}
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
		// No short segment.ms here: it makes brokers answer a waiting fetch at once (measured ~20 ms instead of
		// up to fetch.max.wait), unlike production. The retention scenario sets it at runtime.
		{Name: Topic, Partitions: 3, RF: 3, Configs: map[string]*string{"min.insync.replicas": s("2")}},
		{Name: DLQTopic, Partitions: 3, RF: 3, Configs: map[string]*string{"min.insync.replicas": s("2")}},
		{Name: CompactedTopic, Partitions: 3, RF: 3, Configs: map[string]*string{
			"cleanup.policy": s("compact"), "segment.ms": s("10000"),
			"min.cleanable.dirty.ratio": s("0.01"), "delete.retention.ms": s("10000")}},
	}
}

// ReadAll reads want records of topic from the beginning without a consumer group, so no offsets are committed.
func ReadAll(ctx context.Context, topic string, want int64) ([]Record, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(Brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		return nil, err
	}
	defer cl.Close()
	var out []Record
	for int64(len(out)) < want {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return out, fmt.Errorf("read %d of %d records from %s: %w", len(out), want, topic, err)
		}
		if errs := fs.Errors(); len(errs) > 0 {
			return out, errs[0].Err
		}
		fs.EachRecord(func(r *kgo.Record) { out = append(out, Record{Key: string(r.Key), Value: r.Value}) })
	}
	return out, nil
}
