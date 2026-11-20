// Package observe polls Kafka, the service instances and the DB into one Snapshot per tick.
package observe

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

const prefix = "gap_traffic_data_processing_pipeline_"

// Instance is one service container as the dashboard shows it; counters are from the ingestion pipeline only.
type Instance struct {
	Name         string             `json:"name"`
	Container    string             `json:"container"`
	State        string             `json:"state"`
	Err          string             `json:"err,omitempty"`
	Polled       float64            `json:"polled"`
	Accepted     float64            `json:"accepted"`
	Dedup        float64            `json:"dedup"`
	FilteredDrop float64            `json:"filteredDrop"`
	Failed       map[string]float64 `json:"failed"`
	Partitions   []int32            `json:"partitions"`
}

var states = map[string]string{"0": "RUNNING", "1": "RECOVERING", "2": "PAUSED"}

// ParseMetrics reads Prometheus text and keeps the series of pipeline="short_term".
func ParseMetrics(r io.Reader) (Instance, error) {
	in := Instance{Failed: map[string]float64{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		name, labels, value, ok := splitSample(line)
		if !ok || labels["pipeline"] != "short_term" {
			continue
		}
		switch strings.TrimPrefix(name, prefix) {
		case "state":
			in.State = states[strconv.FormatFloat(value, 'f', 0, 64)]
		case "records_polled_total":
			in.Polled = value
		case "records_accepted_total":
			in.Accepted = value
		case "records_deduplicated_total":
			in.Dedup = value
		case "records_filtered_total":
			if labels["result"] == "drop" {
				in.FilteredDrop = value
			}
		case "records_failed_total":
			in.Failed[labels["error_type"]] = value
		}
	}
	return in, sc.Err()
}

// splitSample parses `name{k="v",...} value`; label values in these metrics contain no commas or quotes.
func splitSample(line string) (string, map[string]string, float64, bool) {
	labels := map[string]string{}
	name, rest, hasLabels := strings.Cut(line, "{")
	if hasLabels {
		var body string
		body, rest, _ = strings.Cut(rest, "}")
		for _, kv := range strings.Split(body, ",") {
			k, v, ok := strings.Cut(kv, "=")
			if ok {
				labels[k] = strings.Trim(v, `"`)
			}
		}
	} else {
		name, rest, _ = strings.Cut(line, " ")
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
	return name, labels, v, err == nil
}
