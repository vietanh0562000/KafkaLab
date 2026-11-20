package observe

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"kafkalab/internal/chaos"
	"kafkalab/internal/kafkax"
)

type KafkaView struct {
	Partitions []kafkax.PartitionInfo
	Group      kafkax.GroupState
	Tracking   kafkax.GroupState
	DLQEnd     int64
}

type Snapshot struct {
	At         time.Time              `json:"at"`
	Containers map[string]string      `json:"containers"`
	Partitions []kafkax.PartitionInfo `json:"partitions"`
	Group      kafkax.GroupState      `json:"group"`
	Tracking   kafkax.GroupState      `json:"tracking"`
	DLQEnd     int64                  `json:"dlqEnd"`
	Instances  []Instance             `json:"instances"`
	DBRows     int64                  `json:"dbRows"`
	LateRows   int64                  `json:"lateRows"`
	Errors     map[string]string      `json:"errors"`
}

// Poller sources are functions so a test can make one hang; every source runs under Timeout, in parallel.
type Poller struct {
	Containers func(ctx context.Context) (map[string]string, error)
	IPs        func(ctx context.Context) (map[string]string, error)
	Kafka      func(ctx context.Context) (KafkaView, error)
	Metrics    func(ctx context.Context, svc string) (Instance, error)
	DB         func(ctx context.Context) (rows, late int64, err error)
	Services   []string
	Timeout    time.Duration

	latest atomic.Pointer[Snapshot]
}

func Services() []string {
	out := make([]string, chaos.MaxSvc)
	for i := range out {
		out[i] = fmt.Sprintf("lab-gap-svc-%d", i+1)
	}
	return out
}

func NewPoller(d *chaos.Docker, a *kafkax.Admin, db *pgxpool.Pool, group func() string) *Poller {
	httpc := &http.Client{}
	return &Poller{
		Containers: d.States,
		IPs:        func(ctx context.Context) (map[string]string, error) { return d.IPs(ctx, "lab-kafka") },
		Kafka: func(ctx context.Context) (KafkaView, error) {
			var v KafkaView
			var err error
			if v.Partitions, err = a.Partitions(ctx, kafkax.Topic); err != nil {
				return v, err
			}
			g := group()
			if v.Group, err = a.Group(ctx, g, kafkax.Topic); err != nil {
				return v, err
			}
			if v.Tracking, err = a.Group(ctx, g+"-tracking", kafkax.Topic); err != nil {
				return v, err
			}
			v.DLQEnd, err = a.EndOffsetSum(ctx, kafkax.DLQTopic)
			return v, err
		},
		Metrics: func(ctx context.Context, svc string) (Instance, error) {
			var n int
			fmt.Sscanf(svc, "lab-gap-svc-%d", &n)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%d/manage/prometheus", 18180+n), nil)
			resp, err := httpc.Do(req)
			if err != nil {
				return Instance{}, err
			}
			defer resp.Body.Close()
			return ParseMetrics(resp.Body)
		},
		DB: func(ctx context.Context) (int64, int64, error) {
			rows, err := count(ctx, db, "traffic_data.traffic_data_sensor_short_term_data")
			if err != nil {
				return 0, 0, err
			}
			late, err := count(ctx, db, "traffic_data.late_arrival_tracking")
			return rows, late, err
		},
		Services: Services(),
		Timeout:  800 * time.Millisecond,
	}
}

func count(ctx context.Context, db *pgxpool.Pool, table string) (int64, error) {
	var n int64
	err := db.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		return 0, nil // the service creates its tables on first start
	}
	return n, err
}

func (p *Poller) Tick(ctx context.Context) Snapshot {
	s := Snapshot{At: time.Now(), Containers: map[string]string{}, Errors: map[string]string{}}
	var (
		mu      sync.Mutex
		wg      sync.WaitGroup
		ips     map[string]string
		metrics = map[string]Instance{}
		mErrs   = map[string]error{}
	)
	fail := func(src string, err error) {
		mu.Lock()
		s.Errors[src] = err.Error()
		mu.Unlock()
	}
	run := func(fn func(ctx context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, cancel := context.WithTimeout(ctx, p.Timeout)
			defer cancel()
			fn(c)
		}()
	}
	run(func(c context.Context) {
		if m, err := p.Containers(c); err != nil {
			fail("containers", err)
		} else {
			mu.Lock()
			s.Containers = m
			mu.Unlock()
		}
	})
	run(func(c context.Context) {
		if m, err := p.IPs(c); err == nil {
			mu.Lock()
			ips = m
			mu.Unlock()
		}
	})
	run(func(c context.Context) {
		v, err := p.Kafka(c)
		if err != nil {
			fail("kafka", err)
		}
		mu.Lock()
		s.Partitions, s.Group, s.Tracking, s.DLQEnd = v.Partitions, v.Group, v.Tracking, v.DLQEnd
		mu.Unlock()
	})
	run(func(c context.Context) {
		rows, late, err := p.DB(c)
		if err != nil {
			fail("db", err)
			return
		}
		mu.Lock()
		s.DBRows, s.LateRows = rows, late
		mu.Unlock()
	})
	// Fetched in parallel with the container list; results of non-running containers are discarded below.
	for _, svc := range p.Services {
		run(func(c context.Context) {
			in, err := p.Metrics(c, svc)
			mu.Lock()
			metrics[svc], mErrs[svc] = in, err
			mu.Unlock()
		})
	}
	wg.Wait()

	owned := map[string][]int32{}
	for _, l := range s.Group.Lags {
		if name, ok := ips[l.OwnerHost]; ok {
			owned[name] = append(owned[name], l.Partition)
		}
	}
	for _, svc := range p.Services {
		state, exists := s.Containers[svc]
		if !exists {
			continue
		}
		in := Instance{Name: svc, Container: state, Partitions: owned[svc]}
		if state == "running" {
			if err := mErrs[svc]; err != nil {
				in.Err = err.Error()
				s.Errors[svc] = err.Error()
			} else {
				m := metrics[svc]
				m.Name, m.Container, m.Partitions = svc, state, owned[svc]
				in = m
			}
		}
		sort.Slice(in.Partitions, func(i, j int) bool { return in.Partitions[i] < in.Partitions[j] })
		s.Instances = append(s.Instances, in)
	}
	p.latest.Store(&s)
	return s
}

func (p *Poller) Run(ctx context.Context, every time.Duration, publish func(Snapshot)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		publish(p.Tick(ctx))
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (p *Poller) Latest() Snapshot {
	if s := p.latest.Load(); s != nil {
		return *s
	}
	return Snapshot{Containers: map[string]string{}, Errors: map[string]string{}}
}
