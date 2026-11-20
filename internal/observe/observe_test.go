package observe

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"kafkalab/internal/kafkax"
)

func TestParseFixture(t *testing.T) {
	f, err := os.Open("testdata/prometheus.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	in, err := ParseMetrics(f)
	if err != nil {
		t.Fatal(err)
	}
	if in.State != "RUNNING" || in.Polled != 5 || in.Accepted != 4 || in.FilteredDrop != 1 ||
		in.Failed["missing_required_fields"] != 1 {
		t.Fatalf("%+v", in)
	}
}

func fakePoller() *Poller {
	return &Poller{
		Containers: func(context.Context) (map[string]string, error) {
			return map[string]string{"lab-kafka-1": "running", "lab-gap-svc-1": "running", "lab-gap-svc-2": "running"}, nil
		},
		IPs: func(context.Context) (map[string]string, error) {
			return map[string]string{"172.20.0.7": "lab-gap-svc-1", "172.20.0.8": "lab-gap-svc-2"}, nil
		},
		Kafka: func(context.Context) (KafkaView, error) {
			return KafkaView{Group: kafkax.GroupState{State: "Stable", Lags: []kafkax.PartitionLag{
				{Partition: 0, OwnerHost: "172.20.0.7"}, {Partition: 1, OwnerHost: "172.20.0.7"}, {Partition: 2, OwnerHost: "172.20.0.8"}}}}, nil
		},
		Metrics: func(_ context.Context, svc string) (Instance, error) {
			return Instance{State: "RUNNING"}, nil
		},
		DB:       func(context.Context) (int64, int64, error) { return 7, 0, nil },
		Services: []string{"lab-gap-svc-1", "lab-gap-svc-2", "lab-gap-svc-3"},
		Timeout:  800 * time.Millisecond,
	}
}

func instance(s Snapshot, name string) Instance {
	for _, in := range s.Instances {
		if in.Name == name {
			return in
		}
	}
	return Instance{}
}

func TestTickSurvivesHangingSource(t *testing.T) {
	p := fakePoller()
	p.DB = func(ctx context.Context) (int64, int64, error) {
		<-ctx.Done()
		return 0, 0, ctx.Err()
	}
	start := time.Now()
	s := p.Tick(context.Background())
	if time.Since(start) > 1100*time.Millisecond {
		t.Fatal("tick stalled")
	}
	if s.Errors["db"] == "" || s.Containers["lab-kafka-1"] != "running" || instance(s, "lab-gap-svc-1").State != "RUNNING" {
		t.Fatalf("%+v", s)
	}
}

func TestOwnersMappedToInstances(t *testing.T) {
	s := fakePoller().Tick(context.Background())
	if got := instance(s, "lab-gap-svc-1").Partitions; !slices.Equal(got, []int32{0, 1}) {
		t.Fatalf("svc-1 %v", got)
	}
	if got := instance(s, "lab-gap-svc-2").Partitions; !slices.Equal(got, []int32{2}) {
		t.Fatalf("svc-2 %v", got)
	}
	if s.DBRows != 7 {
		t.Fatal(s.DBRows)
	}
}

func TestStoppedServiceHasNoMetricsOrError(t *testing.T) {
	p := fakePoller()
	p.Containers = func(context.Context) (map[string]string, error) {
		return map[string]string{"lab-gap-svc-1": "running", "lab-gap-svc-2": "exited"}, nil
	}
	p.Metrics = func(_ context.Context, svc string) (Instance, error) {
		if svc == "lab-gap-svc-2" {
			return Instance{}, context.DeadlineExceeded
		}
		return Instance{State: "RUNNING", Polled: 1}, nil
	}
	s := p.Tick(context.Background())
	stopped := instance(s, "lab-gap-svc-2")
	if stopped.Container != "exited" || stopped.State != "" || stopped.Polled != 0 || s.Errors["lab-gap-svc-2"] != "" {
		t.Fatalf("%+v %v", stopped, s.Errors)
	}
	if _, listed := s.Errors["lab-gap-svc-3"]; listed || instance(s, "lab-gap-svc-3").Name != "" {
		t.Fatal("a service without a container must not be listed")
	}
}
