//go:build integration

package kafkax

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func mustAdmin(t *testing.T) *Admin {
	t.Helper()
	a, err := NewAdmin()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	return a
}

func TestRecreateDescribeProduce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a := mustAdmin(t)
	if n, err := a.BrokerCount(ctx); err != nil || n != 3 {
		t.Fatalf("brokers %d %v", n, err)
	}
	if err := a.RecreateTopics(ctx, LabTopics()); err != nil {
		t.Fatal(err)
	}
	parts, err := a.Partitions(ctx, Topic)
	if err != nil || len(parts) != 3 {
		t.Fatalf("%v %v", parts, err)
	}
	for _, p := range parts {
		if len(p.ISR) != 3 {
			t.Fatalf("partition %d ISR %v", p.Partition, p.ISR)
		}
	}
	cl, err := NewProducer(DefaultProfile())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := make([]Record, 10)
	for i := range recs {
		recs[i] = Record{Key: fmt.Sprintf("k%d", i), Value: []byte("v")}
	}
	for _, r := range ProduceSync(ctx, cl, recs) {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if n, err := a.EndOffsetSum(ctx, Topic); err != nil || n != 10 {
		t.Fatalf("end sum %d %v", n, err)
	}
}

func TestCommitOffsetsOnEmptyGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := mustAdmin(t)
	const g = "itest-group"
	if err := a.CommitOffsets(ctx, g, Topic, map[int32]int64{0: 0, 1: 0, 2: 0}); err != nil {
		t.Fatal(err)
	}
	gs, err := a.Group(ctx, g, Topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(gs.Lags) != 3 {
		t.Fatalf("%+v", gs)
	}
	for _, l := range gs.Lags {
		if l.Committed != 0 {
			t.Fatalf("%+v", l)
		}
	}
	if err := a.DeleteGroup(ctx, g); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteGroup(ctx, g); err != nil {
		t.Fatalf("deleting a missing group must not fail: %v", err)
	}
}

func TestReadAllFromStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := mustAdmin(t)
	if err := a.RecreateTopics(ctx, LabTopics()); err != nil {
		t.Fatal(err)
	}
	cl, err := NewProducer(DefaultProfile())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := []Record{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}, {Key: "c", Value: []byte("3")}}
	for _, r := range ProduceSync(ctx, cl, recs) {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	got, err := ReadAll(ctx, Topic, 3)
	if err != nil || len(got) != 3 {
		t.Fatalf("%v %v", got, err)
	}
}
