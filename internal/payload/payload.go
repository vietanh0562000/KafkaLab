// Package payload builds short-term messages in the envelope the gap service consumes.
package payload

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

const beanType = "de.e21x.data.tp.tdp.ShortTermMeasurementAggregatedDataLane$Bean"

type header struct {
	Source string `json:"source"`
	Tag    string `json:"tag"`
}

type timeBlock struct {
	StartTime        string `json:"startTime"`
	ValidPeriods     []any  `json:"validPeriods"`
	ExceptionPeriods []any  `json:"exceptionPeriods"`
}

type bean struct {
	Type           string         `json:"@type"`
	PermanentID    string         `json:"permanentId,omitempty"`
	RenamedID      string         `json:"permanentID,omitempty"`
	IntervalLength string         `json:"intervalLength,omitempty"`
	Time           timeBlock      `json:"time"`
	LabExtra       map[string]any `json:"labExtra,omitempty"`
}

type envelope struct {
	Header  header `json:"header"`
	Payload bean   `json:"payload"`
}

func SensorID(i int) string { return fmt.Sprintf("lab-sensor-%03d", i) }

func build(seq int64, b bean) []byte {
	b.Type = beanType
	b.Time.ValidPeriods, b.Time.ExceptionPeriods = []any{}, []any{}
	out, err := json.Marshal(envelope{Header: header{Source: "kafka-lab", Tag: "seq-" + strconv.FormatInt(seq, 10)}, Payload: b})
	if err != nil {
		panic(err) // only fixed, serializable types are marshalled
	}
	return out
}

func stamp(t time.Time) timeBlock { return timeBlock{StartTime: t.UTC().Format(time.RFC3339)} }

func ShortTerm(permanentID string, start time.Time, seq int64) []byte {
	return build(seq, bean{PermanentID: permanentID, IntervalLength: "PT1M", Time: stamp(start)})
}

// Poison parses fine but cannot be stored: permanent_id is varchar(255), so the insert fails with SQLState 22001.
func Poison(start time.Time, seq int64) []byte {
	return ShortTerm(strings.Repeat("p", 300), start, seq)
}

func NoInterval(permanentID string, start time.Time, seq int64) []byte {
	return build(seq, bean{PermanentID: permanentID, Time: stamp(start)})
}

// BrokenJSON contains the filter's byte pattern, so it reaches the JSON parser and fails there.
func BrokenJSON(seq int64) []byte {
	return []byte(fmt.Sprintf(`{"header":{"tag":"seq-%d"},"payload":{"intervalLength":"PT1M","permanentId":`, seq))
}

func UnknownField(permanentID string, start time.Time, seq int64) []byte {
	return build(seq, bean{PermanentID: permanentID, IntervalLength: "PT1M", Time: stamp(start),
		LabExtra: map[string]any{"addedBy": "upstream v2"}})
}

// RenamedID mimics an upstream rename; Jackson matching is case-sensitive, so permanentId arrives as null.
func RenamedID(permanentID string, start time.Time, seq int64) []byte {
	return build(seq, bean{RenamedID: permanentID, IntervalLength: "PT1M", Time: stamp(start)})
}

func Seq(value []byte) (int64, bool) {
	var e struct {
		Header header `json:"header"`
	}
	if json.Unmarshal(value, &e) != nil {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(e.Header.Tag, "seq-"), 10, 64)
	return n, err == nil && strings.HasPrefix(e.Header.Tag, "seq-")
}

var berlin = mustLoad("Europe/Berlin")

func mustLoad(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return loc
}

// TodayBerlinUTC is today's midnight in the service's zone; data from today is never classified as late.
func TodayBerlinUTC(now time.Time) time.Time {
	y, m, d := now.In(berlin).Date()
	return time.Date(y, m, d, 0, 0, 0, 0, berlin).UTC()
}

type Point struct {
	SensorID string
	Start    time.Time
}

// Timeline hands out (sensor, minute) pairs that never repeat, so every record is a new DB row.
type Timeline struct {
	cursor time.Time
}

func NewTimeline(start time.Time) *Timeline { return &Timeline{cursor: start} }

func (t *Timeline) Batch(n, sensors int) []Point {
	if sensors < 1 {
		sensors = 1
	}
	points := make([]Point, n)
	for i := range points {
		points[i] = Point{SensorID: SensorID(i % sensors), Start: t.cursor.Add(time.Duration(i/sensors) * time.Minute)}
	}
	t.cursor = t.cursor.Add(time.Duration((n+sensors-1)/sensors) * time.Minute)
	return points
}
