//go:build integration

package kafkax

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
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

func TestTopicLifecycleAndProduceTo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a := mustAdmin(t)
	const name = "itest-pg"
	_ = a.DeleteTopic(ctx, name)
	if err := a.CreateTopic(ctx, TopicSpec{Name: name, Partitions: 2, RF: 3}); err != nil {
		t.Fatal(err)
	}
	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found TopicInfo
	for _, ti := range topics {
		if ti.Name == name {
			found = ti
		}
		if strings.HasPrefix(ti.Name, "__") {
			t.Fatalf("internal topic listed: %s", ti.Name)
		}
	}
	if found.Partitions != 2 || found.RF != 3 {
		t.Fatalf("%+v", found)
	}
	cl, err := NewProducer(DefaultProfile())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	res := ProduceTo(ctx, cl, name, []Record{{Key: "k", Value: []byte("v")}, {Key: "k", Value: []byte("v")}})
	if res[0].Err != nil || res[1].Partition != res[0].Partition || res[1].Offset != res[0].Offset+1 {
		t.Fatalf("%+v", res)
	}
	starts, err := a.StartOffsets(ctx, name)
	if err != nil || starts[res[0].Partition] != 0 {
		t.Fatalf("%v %v", starts, err)
	}
	if err := a.DeleteTopic(ctx, name); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerIDsAndGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := mustAdmin(t)
	ids, err := a.BrokerIDs(ctx)
	if err != nil || len(ids) != 3 || ids[0] != 1 || ids[2] != 3 {
		t.Fatalf("%v %v", ids, err)
	}
	const g = "itest-list-group"
	if err := a.CommitOffsets(ctx, g, Topic, map[int32]int64{0: 0}); err != nil {
		t.Fatal(err)
	}
	defer a.DeleteGroup(ctx, g)
	groups, err := a.ListGroups(ctx)
	if err != nil || !slices.Contains(groups, g) {
		t.Fatalf("%v %v", groups, err)
	}
}

func TestListTopicsSkipsTopicsThatDoNotExist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := mustAdmin(t)
	// The lab's state read asks for one missing topic and for all topics at once; the client may merge them.
	for i := 0; i < 20; i++ {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); a.Leaders(ctx, "itest-missing") }()
		topics, err := a.ListTopics(ctx)
		wg.Wait()
		if err != nil {
			t.Fatal(err)
		}
		for _, ti := range topics {
			if ti.Name == "itest-missing" {
				t.Fatalf("round %d: missing topic listed: %+v", i, topics)
			}
		}
	}
}
