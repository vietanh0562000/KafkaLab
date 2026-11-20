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
	for _, id := range []string{"0.1", "0.2"} {
		if !seen[id] {
			t.Fatalf("scenario %s missing", id)
		}
	}
}
