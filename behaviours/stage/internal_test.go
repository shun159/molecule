package stage

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// flood emits five times as many events as asked for, ever more.
type flood struct{}

func (flood) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (flood) HandleDemand(next, n int) (int, []actor.Effect) {
	events := make([]int, 5*n)
	for i := range events {
		events[i] = next + i
	}
	return next + len(events), actor.Do(Emit[int]{Events: events})
}

// drain takes events until it has had Limit, and then cancels.
type drain struct {
	From  proc.PID
	Limit int
}

func (d drain) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(Subscribe{To: d.From, Max: 7, Min: 2}), nil
}

func (d drain) HandleEvents(n int, events []int, tag any) (int, []actor.Effect) {
	n += len(events)
	if n >= d.Limit {
		return n, actor.Do(Cancel{Tag: tag})
	}
	return n, nil
}

// TestNoMoreThanAsked follows the sends of a subscription: the producer
// never has sent more events than its consumer asked for.
func TestNoMoreThanAsked(t *testing.T) {
	for seed := range uint64(50) {
		s := gensim.New(seed)
		src, _ := gensim.Spawn(s, Produce(flood{}), nil)
		dst, _ := gensim.Spawn(s, Consume(drain{From: src, Limit: 200}), nil)
		s.RunUntilIdle()

		asked, sent := 0, 0
		for _, e := range s.Trace() {
			if e.Kind != gensim.Sent {
				continue
			}
			switch m := e.Msg.(type) {
			case subscribeMsg:
				asked += m.Ask
			case askMsg:
				asked += m.N
			case eventsMsg:
				sent += len(m.Events.([]int))
			}
			if sent > asked {
				t.Fatalf("seed %d: %d events sent, %d asked for", seed, sent, asked)
			}
		}
		if n := stateOf(t, s, dst); n < 200 {
			t.Fatalf("seed %d: the consumer had %d events", seed, n)
		}
	}
}

func stateOf(t *testing.T, s *gensim.Sim, pid proc.PID) int {
	t.Helper()
	c, ok := gensim.State[ConsumerState[int]](s, pid)
	if !ok {
		t.Fatalf("%v is gone", pid)
	}
	return c.State()
}
