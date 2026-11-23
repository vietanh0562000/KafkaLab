//go:build integration

package playground

import (
	"context"
	"testing"
	"time"

	"kafkalab/internal/kafkax"
)

func pollUntil(t *testing.T, ctx context.Context, m *Manager, name string, want int) []RecordView {
	t.Helper()
	var got []RecordView
	for deadline := time.Now().Add(20 * time.Second); len(got) < want && time.Now().Before(deadline); {
		recs, err := m.Poll(ctx, name, 50)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, recs...)
	}
	if len(got) < want {
		t.Fatalf("%s got %d of %d records", name, len(got), want)
	}
	return got
}

func TestCommitDecidesWhatANewMemberReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a, err := kafkax.NewAdmin()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	const topic, group = "itest-pgm", "itest-pgm-g"
	m := New(a)
	defer m.Close()
	_ = m.DeleteTopic(ctx, topic)
	_ = a.DeleteGroup(ctx, group)
	defer a.DeleteGroup(context.Background(), group)
	defer m.DeleteTopic(context.Background(), topic)
	if err := m.CreateTopic(ctx, TopicReq{Name: topic, Partitions: 3, RF: 3, MinISR: 2}); err != nil {
		t.Fatal(err)
	}
	if err := m.AddProducer("p", kafkax.DefaultProfile()); err != nil {
		t.Fatal(err)
	}
	sent, err := m.Send(ctx, "p", topic, "k", "v{i}", 3)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range sent {
		if s.Err != "" || s.Partition != sent[0].Partition || s.Offset != int64(i) {
			t.Fatalf("same key must append to one partition: %+v", sent)
		}
	}

	if err := m.AddConsumer("c1", group, topic, "earliest"); err != nil {
		t.Fatal(err)
	}
	for _, r := range pollUntil(t, ctx, m, "c1", 3) {
		if r.Redelivered {
			t.Fatalf("first read flagged as redelivery: %+v", r)
		}
	}
	// No commit: a new member of the group starts from the beginning again.
	if err := m.RemoveConsumer("c1"); err != nil {
		t.Fatal(err)
	}
	if err := m.AddConsumer("c1", group, topic, "earliest"); err != nil {
		t.Fatal(err)
	}
	for _, r := range pollUntil(t, ctx, m, "c1", 3) {
		if !r.Redelivered {
			t.Fatalf("uncommitted record not flagged on re-read: %+v", r)
		}
	}
	if err := m.Commit(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := m.RemoveConsumer("c1"); err != nil {
		t.Fatal(err)
	}
	if err := m.AddConsumer("c1", group, topic, "earliest"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Send(ctx, "p", topic, "k", "new", 1); err != nil {
		t.Fatal(err)
	}
	got := pollUntil(t, ctx, m, "c1", 1)
	if len(got) != 1 || got[0].Value != "new" || got[0].Redelivered {
		t.Fatalf("after commit only the new record is read: %+v", got)
	}

	// The tail reader starts on the first State call and catches up shortly after.
	st := m.State(ctx, topic)
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if st.Topic != nil && len(st.Topic.Partitions[sent[0].Partition].Records) == 4 {
			break
		}
		st = m.State(ctx, topic)
	}
	if st.Topic == nil || len(st.Topic.Partitions) != 3 || st.Topic.MinISR != 2 {
		t.Fatalf("topic view %+v errors %v", st.Topic, st.Errors)
	}
	p := st.Topic.Partitions[sent[0].Partition]
	if p.End != 4 || len(p.Records) != 4 || p.Records[3].Value != "new" {
		t.Fatalf("partition view %+v", p)
	}
	var g *GroupView
	for i := range st.Groups {
		if st.Groups[i].Name == group {
			g = &st.Groups[i]
		}
	}
	if g == nil || g.Committed[sent[0].Partition] != 3 {
		t.Fatalf("group view %+v", st.Groups)
	}
}

func TestGroupSplitsPartitionsWithoutPolling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	a, err := kafkax.NewAdmin()
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	const topic, group = "itest-pgr", "itest-pgr-g"
	m := New(a)
	defer m.Close()
	_ = m.DeleteTopic(ctx, topic)
	defer a.DeleteGroup(context.Background(), group)
	defer m.DeleteTopic(context.Background(), topic)
	if err := m.CreateTopic(ctx, TopicReq{Name: topic, Partitions: 3, RF: 3, MinISR: 2}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for _, n := range []string{"c1", "c2", "c3", "c4"} {
		if err := m.AddConsumer(n, group, topic, "earliest"); err != nil {
			t.Fatal(err)
		}
	}
	for time.Since(start) < 60*time.Second {
		covered, idle := map[int32]bool{}, 0
		for _, c := range m.State(ctx, topic).Consumers {
			if len(c.Assigned) == 0 {
				idle++
			}
			for _, p := range c.Assigned {
				if covered[p] {
					t.Fatalf("partition %d owned twice", p)
				}
				covered[p] = true
			}
		}
		if len(covered) == 3 && idle == 1 {
			t.Logf("settled after %v", time.Since(start).Round(100*time.Millisecond))
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatal("group never settled to 3 owners + 1 idle without polling")
}
