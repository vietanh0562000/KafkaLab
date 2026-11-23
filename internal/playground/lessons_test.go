package playground

import (
	"strings"
	"testing"
)

func lesson(t *testing.T, id string) Lesson {
	for _, l := range Lessons {
		if l.ID == id {
			return l
		}
	}
	t.Fatalf("no lesson %s", id)
	return Lesson{}
}

func TestLessonCatalog(t *testing.T) {
	for i, l := range Lessons {
		if l.ID == "" || l.Title == "" || l.Question == "" || l.Goal == "" || l.Reveal == "" || l.check == nil {
			t.Fatalf("lesson %d incomplete", i)
		}
		if len(l.Steps) == 0 || len(l.CLI) == 0 || len(l.Confirm) < 2 {
			t.Fatalf("lesson %s needs steps, CLI and >=2 confirm questions", l.ID)
		}
		for _, c := range l.CLI {
			if !strings.HasPrefix(c, "docker ") {
				t.Fatalf("lesson %s: CLI must run against the lab as-is: %q", l.ID, c)
			}
		}
	}
}

func topicWith(parts ...PartitionView) *TopicView {
	return &TopicView{Name: LessonTopic, Partitions: parts}
}

func TestLesson2NeedsStableKeyAndSpread(t *testing.T) {
	sends := func(kp ...any) []SendView {
		var out []SendView
		for i := 0; i < len(kp); i += 2 {
			out = append(out, SendView{Topic: LessonTopic, Key: kp[i].(string), Partition: int32(kp[i+1].(int))})
		}
		return out
	}
	l := lesson(t, "2")
	if ok, _ := l.check(LessonInput{Facts: Facts{Sends: sends("a", 0, "a", 0, "a", 0)}}); ok {
		t.Fatal("one partition only is not a spread")
	}
	if ok, d := l.check(LessonInput{Facts: Facts{Sends: sends("a", 0, "a", 0, "a", 0, "b", 2)}}); !ok {
		t.Fatal(d)
	}
	// Records with no key, or failed sends, say nothing about key hashing.
	if ok, _ := l.check(LessonInput{Facts: Facts{Sends: append(sends("", 0, "", 0, "", 0, "b", 2),
		SendView{Topic: LessonTopic, Key: "a", Partition: 1, Err: "x"})}}); ok {
		t.Fatal("keyless or failed sends counted")
	}
}

func TestLesson5NeedsIdleConsumerAndFullCoverage(t *testing.T) {
	l := lesson(t, "5")
	tv := topicWith(PartitionView{Partition: 0}, PartitionView{Partition: 1}, PartitionView{Partition: 2})
	c := func(name string, ps ...int32) ConsumerView {
		return ConsumerView{Name: name, Group: "g1", Topic: LessonTopic, Assigned: ps}
	}
	// Mid-rebalance: four members but partitions not yet handed out.
	if ok, _ := l.check(LessonInput{Topic: tv, Consumers: []ConsumerView{c("c1", 0), c("c2"), c("c3"), c("c4")}}); ok {
		t.Fatal("uncovered partitions accepted")
	}
	if ok, d := l.check(LessonInput{Topic: tv, Consumers: []ConsumerView{c("c1", 0), c("c2", 1), c("c3", 2), c("c4")}}); !ok {
		t.Fatal(d)
	}
}

func TestLesson8NeedsBothOutcomesWithTwoBrokersDown(t *testing.T) {
	l := lesson(t, "8")
	f := newFacts()
	f.recordSends([]SendView{{Topic: LessonTopic, Acks: "all", BrokersDown: 1, Err: "timeout"}})
	f.recordSends([]SendView{{Topic: LessonTopic, Acks: "1", BrokersDown: 1}})
	if ok, _ := l.check(LessonInput{Facts: f.snapshot()}); ok {
		t.Fatal("one broker down is lesson 7, not 8")
	}
	f.recordSends([]SendView{{Topic: LessonTopic, Acks: "all", BrokersDown: 2, Err: "NOT_ENOUGH_REPLICAS"}})
	f.recordSends([]SendView{{Topic: LessonTopic, Acks: "1", BrokersDown: 2}})
	if ok, d := l.check(LessonInput{Facts: f.snapshot()}); !ok || !strings.Contains(d, "NOT_ENOUGH_REPLICAS") {
		t.Fatal(d)
	}
}

func TestLessonsLatchDone(t *testing.T) {
	m := New(nil)
	f := newFacts()
	f.set(LessonTopic, factRedelivered)
	f.set(LessonTopic, factCommitted)
	if st := m.checkLessons(LessonInput{Facts: f.snapshot()}); !st[3].Done {
		t.Fatal("lesson 4 should pass")
	}
	if st := m.checkLessons(LessonInput{Facts: newFacts().snapshot()}); !st[3].Done {
		t.Fatal("a passed lesson must stay done")
	}
}

func TestLesson1NeedsTheReplicationLaterLessonsUse(t *testing.T) {
	l := lesson(t, "1")
	p := func(n int32, rf int) PartitionView {
		reps := []int32{1, 2, 3}[:rf]
		return PartitionView{Partition: n, Replicas: reps, ISR: reps, Start: 0, End: 3}
	}
	tv := topicWith(p(0, 1), p(1, 1), p(2, 1))
	tv.MinISR = 1
	// Lessons 7 and 8 kill brokers; with RF 1 they cannot pass, so lesson 1 must not let RF 1 through.
	if ok, d := l.check(LessonInput{Topic: tv}); ok || !strings.Contains(d, "RF") {
		t.Fatal(d)
	}
	tv = topicWith(p(0, 3), p(1, 3), p(2, 3))
	tv.MinISR = 2
	if ok, d := l.check(LessonInput{Topic: tv}); !ok {
		t.Fatal(d)
	}
}
