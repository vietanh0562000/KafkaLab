package playground

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

var legalName = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// validateTopicName keeps the playground off internal topics and the scenarios' topics.
func validateTopicName(name string) error {
	switch {
	case name == "" || len(name) > 249 || !legalName.MatchString(name):
		return fmt.Errorf("topic name must be 1-249 chars of a-z A-Z 0-9 . _ -")
	case strings.HasPrefix(name, "__"):
		return fmt.Errorf("%s is an internal topic", name)
	case strings.HasPrefix(name, "lab.") || strings.HasPrefix(name, "dlq-lab."):
		return fmt.Errorf("%s belongs to the scenarios; use another name", name)
	}
	return nil
}

var clientName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,40}$`)

func validateClientName(name string) error {
	if !clientName.MatchString(name) {
		return fmt.Errorf("name must be 1-40 chars of a-z A-Z 0-9 _ -")
	}
	return nil
}

// deliveryTracker remembers the highest offset each group has received per partition, so a record delivered
// again to the same group (after a crash or rebalance without commit) can be flagged.
type deliveryTracker struct {
	mu  sync.Mutex
	max map[string]int64
}

func newDeliveryTracker() *deliveryTracker { return &deliveryTracker{max: map[string]int64{}} }

func trackerKey(group, topic string, partition int32) string {
	return fmt.Sprintf("%s\x00%s\x00%d", group, topic, partition)
}

func (t *deliveryTracker) observe(group, topic string, partition int32, offset int64) (redelivered bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	k := trackerKey(group, topic, partition)
	prev, seen := t.max[k]
	if seen && offset <= prev {
		return true
	}
	t.max[k] = offset
	return false
}

func (t *deliveryTracker) forgetTopic(topic string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k := range t.max {
		if strings.Split(k, "\x00")[1] == topic {
			delete(t.max, k)
		}
	}
}

func (t *deliveryTracker) reset() {
	t.mu.Lock()
	t.max = map[string]int64{}
	t.mu.Unlock()
}

type ring[T any] struct {
	buf []T
	cap int
}

func newRing[T any](n int) *ring[T] { return &ring[T]{cap: n} }

func (r *ring[T]) push(v T) {
	r.buf = append(r.buf, v)
	if len(r.buf) > r.cap {
		r.buf = r.buf[len(r.buf)-r.cap:]
	}
}

func (r *ring[T]) items() []T { return append([]T(nil), r.buf...) }

// replace swaps the first item matching for v; an item already pushed out of the ring is ignored.
func (r *ring[T]) replace(match func(T) bool, v T) {
	for i := range r.buf {
		if match(r.buf[i]) {
			r.buf[i] = v
			return
		}
	}
}

// trimTail keeps records inside [start, end): anything else belongs to an older incarnation of the topic.
// Unknown offsets (-1, leader unreachable) keep everything.
func trimTail(recs []RecordView, start, end int64) []RecordView {
	if start < 0 || end < 0 {
		return recs
	}
	var out []RecordView
	for _, r := range recs {
		if r.Offset >= start && r.Offset < end {
			out = append(out, r)
		}
	}
	return out
}
