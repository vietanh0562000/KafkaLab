package payload

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 19, 23, 0, 0, 0, time.UTC)

const needle = `"intervalLength":"PT1M"`

func permanentID(t *testing.T, b []byte) string {
	t.Helper()
	var v struct {
		Payload struct {
			PermanentID string `json:"permanentId"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v.Payload.PermanentID
}

func TestShortTermValidWithSeq(t *testing.T) {
	b := ShortTerm("lab-sensor-001", t0, 42)
	if !json.Valid(b) || !bytes.Contains(b, []byte(needle)) {
		t.Fatal(string(b))
	}
	if s, ok := Seq(b); !ok || s != 42 {
		t.Fatal(s, ok)
	}
	if !bytes.Contains(b, []byte(`"startTime":"2026-11-19T23:00:00Z"`)) {
		t.Fatal(string(b))
	}
	if permanentID(t, b) != "lab-sensor-001" {
		t.Fatal(string(b))
	}
}

func TestPoisonID300(t *testing.T) {
	b := Poison(t0, 1)
	if got := len(permanentID(t, b)); got != 300 {
		t.Fatalf("len %d", got)
	}
	if !bytes.Contains(b, []byte(needle)) {
		t.Fatal("poison must pass the interval filter")
	}
}

func TestNoIntervalLacksField(t *testing.T) {
	b := NoInterval("lab-sensor-001", t0, 1)
	if !json.Valid(b) || bytes.Contains(b, []byte(`"intervalLength"`)) {
		t.Fatal(string(b))
	}
}

func TestBrokenJSONHasNeedleButInvalid(t *testing.T) {
	b := BrokenJSON(1)
	if !bytes.Contains(b, []byte(needle)) || json.Valid(b) {
		t.Fatal(string(b))
	}
}

func TestRenamedIDHasNoPermanentId(t *testing.T) {
	b := RenamedID("lab-sensor-001", t0, 1)
	if !json.Valid(b) || bytes.Contains(b, []byte(`"permanentId"`)) || !bytes.Contains(b, []byte(`"permanentID"`)) {
		t.Fatal(string(b))
	}
}

func TestUnknownFieldStillValid(t *testing.T) {
	b := UnknownField("lab-sensor-001", t0, 1)
	if !json.Valid(b) || !bytes.Contains(b, []byte(`"labExtra"`)) || permanentID(t, b) != "lab-sensor-001" {
		t.Fatal(string(b))
	}
}

func TestTodayBerlinUTC(t *testing.T) {
	got := TodayBerlinUTC(time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC))
	if !got.Equal(time.Date(2026, 11, 19, 23, 0, 0, 0, time.UTC)) {
		t.Fatal(got)
	}
}

func TestTimelineNeverRepeats(t *testing.T) {
	tl := NewTimeline(t0)
	seen := map[Point]bool{}
	for _, p := range append(tl.Batch(7, 3), tl.Batch(5, 2)...) {
		if seen[p] {
			t.Fatalf("duplicate %v", p)
		}
		seen[p] = true
	}
	got := NewTimeline(t0).Batch(4, 3)
	want := []Point{{"lab-sensor-000", t0}, {"lab-sensor-001", t0}, {"lab-sensor-002", t0}, {"lab-sensor-000", t0.Add(time.Minute)}}
	if !slices.Equal(got, want) {
		t.Fatalf("%v", got)
	}
}
