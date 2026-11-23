package playground

import (
	"context"
	"strings"
	"testing"
	"time"

	"kafkalab/internal/kafkax"
)

func TestValidateTopicName(t *testing.T) {
	for _, ok := range []string{"bai-1", "orders.v2", "a_b"} {
		if err := validateTopicName(ok); err != nil {
			t.Fatalf("%s: %v", ok, err)
		}
	}
	// Internal and scenario topics are not the playground's to create or delete.
	for _, bad := range []string{"", "__consumer_offsets", "lab.short-term", "dlq-lab.short-term", "a b", strings.Repeat("x", 250)} {
		if err := validateTopicName(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}

func TestDeliveryTrackerCountsRedelivery(t *testing.T) {
	tr := newDeliveryTracker()
	if tr.observe("g4", "t", 0, 5) || tr.observe("g4", "t", 0, 6) {
		t.Fatal("first deliveries are not redeliveries")
	}
	if !tr.observe("g4", "t", 0, 5) {
		t.Fatal("offset 5 delivered twice to g4 is a redelivery")
	}
	if tr.observe("g5", "t", 0, 5) || tr.observe("g4", "t", 1, 5) {
		t.Fatal("another group or partition has its own history")
	}
	tr.forgetTopic("t")
	if tr.observe("g4", "t", 0, 5) {
		t.Fatal("a recreated topic starts a new history")
	}
}

func TestRingKeepsLast(t *testing.T) {
	r := newRing[int](3)
	for i := 1; i <= 5; i++ {
		r.push(i)
	}
	if got := r.items(); len(got) != 3 || got[0] != 3 || got[2] != 5 {
		t.Fatal(got)
	}
}

func TestTrimTailDropsRecordsOutsideLog(t *testing.T) {
	var recs []RecordView
	for i := int64(0); i < 10; i++ {
		recs = append(recs, RecordView{Offset: i})
	}
	// After a delete-and-recreate the old incarnation's records lie beyond the new end offset.
	got := trimTail(recs, 0, 3)
	if len(got) != 3 || got[2].Offset != 2 {
		t.Fatal(got)
	}
}

func TestNamesUniqueAndProfilesValid(t *testing.T) {
	m := New(nil)
	defer m.Close()
	if err := m.AddProducer("p1", kafkax.DefaultProfile()); err != nil {
		t.Fatal(err)
	}
	if err := m.AddProducer("p1", kafkax.DefaultProfile()); err == nil {
		t.Fatal("duplicate producer accepted")
	}
	if err := m.AddProducer("p2", kafkax.ProducerProfile{Acks: "1", Idempotent: true}); err == nil {
		t.Fatal("idempotence without acks=all accepted")
	}
	if _, err := m.Send(nil, "nope", "t", "k", "v", 1); err == nil {
		t.Fatal("unknown producer accepted")
	}
	if err := m.AddConsumer("c1", "", "t", "earliest"); err == nil {
		t.Fatal("empty group accepted")
	}
	if err := m.AddConsumer("c1", "g", "t", "middle"); err == nil {
		t.Fatal("unknown auto.offset.reset accepted")
	}
}

func TestTrimTailKeepsRecordsWhenOffsetsUnknown(t *testing.T) {
	recs := []RecordView{{Offset: 4}, {Offset: 5}}
	// While a leader is unreachable its offsets are unknown (-1); the last records seen are still the best view.
	if got := trimTail(recs, -1, -1); len(got) != 2 {
		t.Fatal(got)
	}
}

func TestSendShowsPendingUntilAcked(t *testing.T) {
	m := New(nil)
	defer m.Close()
	release := make(chan struct{})
	m.produce = func(ctx context.Context, p *producer, topic string, recs []kafkax.Record) []kafkax.Result {
		<-release
		return []kafkax.Result{{Partition: 1, Offset: 9}}
	}
	if err := m.AddProducer("p1", kafkax.DefaultProfile()); err != nil {
		t.Fatal(err)
	}
	done := make(chan []SendView)
	go func() {
		res, _ := m.Send(context.Background(), "p1", "orders", "k", "v", 1)
		done <- res
	}()
	var pending ProducerView
	for i := 0; i < 100; i++ {
		_, cs := m.clients()
		_ = cs
		if pending = m.producers["p1"].view(); len(pending.Sends) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(pending.Sends) != 1 || !pending.Sends[0].Pending {
		t.Fatalf("a send in flight must show as pending: %+v", pending.Sends)
	}
	close(release)
	res := <-done
	got := m.producers["p1"].view().Sends
	if res[0].Pending || len(got) != 1 || got[0].Pending || got[0].Partition != 1 || got[0].Offset != 9 {
		t.Fatalf("pending entry must be replaced by the result: %+v %+v", res, got)
	}
}
