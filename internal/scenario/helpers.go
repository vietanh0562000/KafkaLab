package scenario

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"kafkalab/internal/kafkax"
	"kafkalab/internal/observe"
)

const cliPrefix = "docker exec lab-kafka-1 /opt/kafka/bin/"

func cli(tool, args string) string {
	return cliPrefix + tool + " --bootstrap-server kafka-1:9092 " + args
}

var (
	cliDescribeTopic = cli("kafka-topics.sh", "--describe --topic lab.short-term")
	cliDescribeGroup = cli("kafka-consumer-groups.sh", "--describe --group gap-lab")
)

func lagSum(s observe.Snapshot) int64 {
	var n int64
	for _, l := range s.Group.Lags {
		n += l.Lag
	}
	return n
}

func instance(s observe.Snapshot, name string) (observe.Instance, bool) {
	for _, in := range s.Instances {
		if in.Name == name {
			return in, true
		}
	}
	return observe.Instance{}, false
}

func addInt(c *Ctx, key string, n int) int {
	v, _ := c.Vars[key].(int)
	c.Vars[key] = v + n
	return v + n
}

// send produces recs with the default profile and fails the step if any record was not acknowledged.
func send(ctx context.Context, c *Ctx, recs []kafkax.Record) ([]kafkax.Result, error) {
	res, err := c.Lab.ProduceRecords(ctx, kafkax.DefaultProfile(), recs)
	if err != nil {
		return nil, err
	}
	var errs []error
	for _, r := range res {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	}
	if len(errs) > 0 {
		return res, fmt.Errorf("%d of %d records not acknowledged: %w", len(errs), len(recs), errors.Join(errs[:min(3, len(errs))]...))
	}
	addInt(c, "produced", len(recs))
	return res, nil
}

// produceGood sends n fresh records over the given number of sensors.
func produceGood(n, sensors int) func(context.Context, *Ctx) error {
	return func(ctx context.Context, c *Ctx) error {
		_, err := send(ctx, c, c.Lab.NextRecords(n, sensors))
		return err
	}
}

func partitionSummary(s observe.Snapshot) string {
	var parts []string
	for _, l := range s.Group.Lags {
		parts = append(parts, fmt.Sprintf("P%d committed %d / end %d", l.Partition, l.Committed, l.End))
	}
	return strings.Join(parts, ", ")
}

func percentile(ms []int64, p float64) int64 {
	if len(ms) == 0 {
		return 0
	}
	sorted := append([]int64(nil), ms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
