package playground

import (
	"sort"
	"strings"
	"sync"
)

type fact string

const (
	factCommitted   fact = "committed"
	factRedelivered fact = "redelivered"
	factIsrShrunk   fact = "isrShrunk"
	// acks=all accepted while at least one broker was down.
	factAcksAllWithBrokerDown fact = "acksAllWithBrokerDown"
	// With two brokers down: acks=all rejected, acks=1 or 0 accepted.
	factAcksAllRejected fact = "acksAllRejected"
	factAcksOneAccepted fact = "acksOneAccepted"
)

// facts remembers what the learner has made happen, for lesson checks that depend on history, not on state.
type facts struct {
	mu     sync.Mutex
	flags  map[string]bool
	sends  *ring[SendView]
	groups map[string]map[string]bool
}

func newFacts() *facts {
	return &facts{flags: map[string]bool{}, sends: newRing[SendView](200), groups: map[string]map[string]bool{}}
}

func flagKey(topic string, f fact) string { return topic + "|" + string(f) }

func (f *facts) set(topic string, k fact) {
	f.mu.Lock()
	f.flags[flagKey(topic, k)] = true
	f.mu.Unlock()
}

func (f *facts) recordSends(svs []SendView) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, sv := range svs {
		f.sends.push(sv)
		ok := sv.Err == ""
		switch {
		case ok && sv.Acks == "all" && sv.BrokersDown >= 1:
			f.flags[flagKey(sv.Topic, factAcksAllWithBrokerDown)] = true
		}
		if sv.BrokersDown >= 2 {
			// Only a replica shortage counts; a send failed by deleting its producer says nothing about acks.
			if !ok && sv.Acks == "all" && (strings.Contains(sv.Err, "timed out") || strings.Contains(sv.Err, "NOT_ENOUGH_REPLICAS")) {
				f.flags[flagKey(sv.Topic, factAcksAllRejected)] = true
			}
			if ok && sv.Acks != "all" {
				f.flags[flagKey(sv.Topic, factAcksOneAccepted)] = true
			}
		}
	}
}

// reset clears history in place; the pointer is shared with readers that do not hold the manager's lock.
func (f *facts) reset() {
	f.mu.Lock()
	f.flags, f.sends, f.groups = map[string]bool{}, newRing[SendView](200), map[string]map[string]bool{}
	f.mu.Unlock()
}

func (f *facts) recordPoll(group, topic string, recs []RecordView) {
	if len(recs) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groups[topic] == nil {
		f.groups[topic] = map[string]bool{}
	}
	f.groups[topic][group] = true
	for _, r := range recs {
		if r.Redelivered {
			f.flags[flagKey(topic, factRedelivered)] = true
		}
	}
}

// Facts is a copy of the history a lesson check may read.
type Facts struct {
	flags  map[string]bool
	Sends  []SendView
	groups map[string][]string
}

func (f Facts) Has(topic string, k fact) bool { return f.flags[flagKey(topic, k)] }

// GroupsThatRead lists the groups that received at least one record of topic.
func (f Facts) GroupsThatRead(topic string) []string { return f.groups[topic] }

func (f *facts) snapshot() Facts {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := Facts{flags: map[string]bool{}, Sends: f.sends.items(), groups: map[string][]string{}}
	for k, v := range f.flags {
		out.flags[k] = v
	}
	for t, gs := range f.groups {
		for g := range gs {
			out.groups[t] = append(out.groups[t], g)
		}
		sort.Strings(out.groups[t])
	}
	return out
}
