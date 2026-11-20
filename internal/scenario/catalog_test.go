package scenario

import (
	"strings"
	"testing"
)

func TestCatalogHasTeachingText(t *testing.T) {
	seen := map[string]bool{}
	for _, s := range All() {
		if seen[s.ID] {
			t.Fatalf("duplicate id %s", s.ID)
		}
		seen[s.ID] = true
		if !strings.HasPrefix(s.Module, "M") || s.Title == "" || s.Question == "" || s.Reveal == "" {
			t.Fatalf("%s: missing module/title/question/reveal", s.ID)
		}
		if len(s.CLI) == 0 || len(s.Confirm) < 2 || len(s.Steps) == 0 || s.Baseline.Instances < 1 {
			t.Fatalf("%s: needs CLI, >=2 confirm questions, steps and a baseline", s.ID)
		}
		for _, c := range s.CLI {
			if !strings.HasPrefix(c, "docker exec lab-kafka-1 /opt/kafka/bin/") && !strings.HasPrefix(c, "docker ") {
				t.Fatalf("%s: CLI must be runnable as-is against the lab: %q", s.ID, c)
			}
		}
	}
	for _, id := range []string{"0.1", "0.2", "3.1", "3.2", "3.3", "3.4"} {
		if !seen[id] {
			t.Fatalf("scenario %s missing", id)
		}
	}
}

// Claims a review found false against the service code; they must not come back.
func TestRevealsAvoidKnownFalseClaims(t *testing.T) {
	banned := []string{
		"COPY không cho biết",                  // the DAO does identify the offending row (identifyOffendingRecords)
		"không cho biết dòng nào",              // same claim, other wording
		"bị từ chối ngay",                      // a stopped container fails DNS (unknown host), not a refused connection
		"FilteredKafkaDataSource.java</code>)", // the success ack lives in IngestionPipeline
		"khi DB mới là nút thắt",               // never measured
	}
	for _, s := range All() {
		text := s.Reveal + strings.Join(s.Confirm, " ")
		for _, b := range banned {
			if strings.Contains(text, b) {
				t.Errorf("%s still says %q", s.ID, b)
			}
		}
	}
}
