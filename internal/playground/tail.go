package playground

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/internal/kafkax"
)

const tailSize = 12

// tail keeps the last records of every partition of one topic, read without a group so nothing is committed.
type tail struct {
	cl     *kgo.Client
	cancel context.CancelFunc
	mu     sync.Mutex
	recs   map[int32]*ring[RecordView]
}

func newTail(topic string) (*tail, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkax.Brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd().Relative(-tailSize)),
		kgo.FetchMaxWait(300*time.Millisecond), kgo.MetadataMinAge(time.Second))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	t := &tail{cl: cl, cancel: cancel, recs: map[int32]*ring[RecordView]{}}
	go func() {
		for ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			t.mu.Lock()
			fs.EachRecord(func(r *kgo.Record) {
				rg, ok := t.recs[r.Partition]
				if !ok {
					rg = newRing[RecordView](tailSize)
					t.recs[r.Partition] = rg
				}
				rg.push(RecordView{Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: excerpt(r.Value)})
			})
			t.mu.Unlock()
		}
	}()
	return t, nil
}

func (t *tail) records(p int32) []RecordView {
	t.mu.Lock()
	defer t.mu.Unlock()
	if rg, ok := t.recs[p]; ok {
		out := rg.items()
		sort.Slice(out, func(i, j int) bool { return out[i].Offset < out[j].Offset })
		return out
	}
	return nil
}

func (t *tail) close() {
	t.cancel()
	t.cl.Close()
}
