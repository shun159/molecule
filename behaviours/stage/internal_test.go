package stage

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// flood emits five times as many events as asked for, ever more.
type flood struct{ Dispatch Dispatcher }

func (f flood) Dispatcher() Dispatcher { return f.Dispatch }

func (flood) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (flood) HandleDemand(next, n int) (int, []actor.Effect) {
	events := make([]int, 5*n)
	for i := range events {
		events[i] = next + i
	}
	return next + len(events), actor.Do(Emit[int]{Events: events})
}

// drain takes events, of Partition if any, until it has had Limit, and
// then cancels.
type drain struct {
	From      proc.PID
	Limit     int
	Partition int
}

func (d drain) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(Subscribe{To: d.From, Max: 7, Min: 2, Partition: d.Partition}), nil
}

func (d drain) HandleEvents(n int, events []int, tag any) (int, []actor.Effect) {
	n += len(events)
	if n >= d.Limit {
		return n, actor.Do(Cancel{Tag: tag})
	}
	return n, nil
}

// triple emits three of each event.
type triple struct{ From proc.PID }

func (t triple) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(Subscribe{To: t.From, Max: 5, Min: 1}), nil
}

func (triple) HandleEvents(n int, events []int, _ any) (int, []actor.Effect) {
	var out []int
	for _, e := range events {
		out = append(out, e, e, e)
	}
	return n + len(events), actor.Do(Emit[int]{Events: out})
}

// TestNoMoreThanAsked follows the sends of each subscription of a
// pipeline, its stages feeding several consumers: no producer ever has
// sent a consumer more events than it asked for.
func TestNoMoreThanAsked(t *testing.T) {
	for seed := range uint64(50) {
		s := gensim.New(seed)
		src, _ := gensim.Spawn(s, Produce(flood{}), nil)
		mid, _ := gensim.Spawn(s, ProduceConsume(triple{From: src}), nil)
		var dsts []proc.PID
		for _, from := range []proc.PID{src, mid, mid} {
			dst, _ := gensim.Spawn(s, Consume(drain{From: from, Limit: 200}), nil)
			dsts = append(dsts, dst)
		}
		s.RunUntilIdle()

		if n := subscriptionsSent(t, s); n != 4 {
			t.Fatalf("seed %d: events sent on %d subscriptions", seed, n)
		}
		for _, dst := range dsts {
			if n := stateOf(t, s, dst); n < 200 {
				t.Fatalf("seed %d: %v had %d events", seed, dst, n)
			}
		}
	}
}

// TestNoMoreThanAskedDispatchers checks the dispatchers other than
// Demand likewise, with a producer feeding three consumers.
func TestNoMoreThanAskedDispatchers(t *testing.T) {
	dispatchers := map[string]Dispatcher{
		"broadcast": Broadcast{},
		"partition": Partition[int]{Partitions: 3, Hash: func(n int) int { return n % 3 }},
	}
	for name, d := range dispatchers {
		for seed := range uint64(30) {
			s := gensim.New(seed)
			src, _ := gensim.Spawn(s, Produce(flood{Dispatch: d}), nil)
			for k := range 3 {
				gensim.Spawn(s, Consume(drain{From: src, Limit: 200, Partition: k}), nil)
			}
			s.RunUntilIdle()

			if n := subscriptionsSent(t, s); n != 3 {
				t.Fatalf("%s seed %d: events sent on %d subscriptions", name, seed, n)
			}
		}
	}
}

// subscriptionsSent follows the sends of each subscription in the trace
// of s, failing t if a producer sends more events than asked for, and
// returns on how many subscriptions events were sent.
func subscriptionsSent(t *testing.T, s *gensim.Sim) int {
	t.Helper()
	asked, sent := map[subRef]int{}, map[subRef]int{}
	for _, e := range s.Trace() {
		if e.Kind != gensim.Sent {
			continue
		}
		switch m := e.Msg.(type) {
		case subscribeMsg:
			asked[m.Ref] += m.Ask
		case askMsg:
			asked[m.Ref] += m.N
		case eventsMsg:
			sent[m.Ref] += len(m.Events.([]int))
			if sent[m.Ref] > asked[m.Ref] {
				t.Fatalf("%d events sent %v, %d asked for", sent[m.Ref], m.Ref, asked[m.Ref])
			}
		}
	}
	return len(sent)
}

func stateOf(t *testing.T, s *gensim.Sim, pid proc.PID) int {
	t.Helper()
	c, ok := gensim.State[ConsumerState[int]](s, pid)
	if !ok {
		t.Fatalf("%v is gone", pid)
	}
	return c.State()
}
