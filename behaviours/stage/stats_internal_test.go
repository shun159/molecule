package stage

import (
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

func statsOf(t *testing.T, s *gensim.Sim, pid proc.PID) Stats {
	t.Helper()
	v, err := s.Call(pid, statsReq{})
	if err != nil {
		t.Fatal(err)
	}
	return v.(Stats)
}

// TestStats has the stages of a pipeline tell where its events are, at
// rest: the demand of a producer that of its consumers, what they
// received what it emitted.
func TestStats(t *testing.T) {
	for seed := range uint64(20) {
		pipelineStats(t, seed)
	}
}

func pipelineStats(t *testing.T, seed uint64) {
	t.Helper()
	s := gensim.New(seed)
	src := spawnStage(t, s, Produce(seq{Limit: 100, Over: 3, Open: true}))
	got := spawnStage(t, s, Consume(collect{From: src, Max: 10}))
	held := spawnStage(t, s, ProduceConsume(pass{From: src, Max: 10})) // with no consumer
	s.RunUntilIdle()

	p, g, h := statsOf(t, s, src), statsOf(t, s, got), statsOf(t, s, held)
	if p.Consumers != 2 || p.Emitted != 100 || p.Held != 0 {
		t.Errorf("producer %+v", p)
	}
	if p.Demand != g.Pending+h.Pending {
		t.Errorf("demand %d, pending %d and %d", p.Demand, g.Pending, h.Pending)
	}
	if g.Received+h.Received != p.Emitted {
		t.Errorf("received %d and %d of %d", g.Received, h.Received, p.Emitted)
	}
	if g.Subscriptions != 1 || g.Queued != 0 || g.Received != uint64(stateLen(t, s, got)) {
		t.Errorf("consumer %+v", g)
	}
	// It asked for 10, had them, and handled none: no consumer wants them.
	if want := (Stats{Subscriptions: 1, Received: 10, Queued: 10}); h != want {
		t.Errorf("producer-consumer %+v, want %+v", h, want)
	}
}

func stateLen(t *testing.T, s *gensim.Sim, pid proc.PID) int {
	t.Helper()
	c, _ := gensim.State[ConsumerState[[]int]](s, pid)
	return len(c.State())
}

// TestStatsWaiting has a consumer Resubscribe to a producer gone: it is
// told waiting.
func TestStatsWaiting(t *testing.T) {
	s := gensim.New(1)
	sub := Subscribe{To: actor.Local("none"), Ending: Resubscribe}
	pid := spawnStage(t, s, Consume(subscriber{sub}))
	s.RunUntilIdle()
	if st := statsOf(t, s, pid); st.Waiting != 1 || st.Subscriptions != 0 {
		t.Fatalf("%+v", st)
	}
}

// subscriber subscribes as it is given.
type subscriber struct{ Sub Subscribe }

func (c subscriber) Init(proc.PID) (int, []actor.Effect, error) { return 0, actor.Do(c.Sub), nil }

func (subscriber) HandleEvents(n int, events []int, _ any) (int, []actor.Effect) {
	return n + len(events), nil
}
