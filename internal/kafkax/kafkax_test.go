package kafkax

import (
	"slices"
	"testing"
)

func TestJavaProps(t *testing.T) {
	if got := DefaultProfile().JavaProps(); !slices.Equal(got, []string{"acks=all", "enable.idempotence=true"}) {
		t.Fatal(got)
	}
	if got := (ProducerProfile{Acks: "1"}).JavaProps(); !slices.Equal(got, []string{"acks=1", "enable.idempotence=false"}) {
		t.Fatal(got)
	}
	got := (ProducerProfile{Acks: "all", Idempotent: true, TxnID: "t1"}).JavaProps()
	if !slices.Contains(got, "transactional.id=t1") {
		t.Fatal(got)
	}
}

func TestIdempotenceNeedsAcksAll(t *testing.T) {
	if _, err := NewProducer(ProducerProfile{Acks: "1", Idempotent: true}); err == nil {
		t.Fatal("want error")
	}
}

func TestLabTopics(t *testing.T) {
	specs := LabTopics()
	if len(specs) != 3 {
		t.Fatalf("got %d specs", len(specs))
	}
	byName := map[string]TopicSpec{}
	for _, s := range specs {
		byName[s.Name] = s
	}
	st := byName[Topic]
	if st.Partitions != 3 || st.RF != 3 || *st.Configs["min.insync.replicas"] != "2" || *st.Configs["segment.ms"] != "10000" {
		t.Fatalf("%+v", st)
	}
	if d := byName[DLQTopic]; d.Partitions != 3 || d.RF != 3 {
		t.Fatalf("%+v", d)
	}
	if c := byName[CompactedTopic]; *c.Configs["cleanup.policy"] != "compact" {
		t.Fatalf("%+v", c)
	}
}
