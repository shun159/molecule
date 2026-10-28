package stage

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// seq emits the integers below Limit, Over times as many as asked for,
// and is then done, unless Open.
type seq struct {
	Limit    int
	Over     int
	Dispatch Dispatcher
	Open     bool
}

func (q seq) Dispatcher() Dispatcher { return q.Dispatch }

func (seq) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (q seq) HandleDemand(next, n int) (int, []actor.Effect) {
	k := min(n*q.Over, q.Limit-next)
	events := make([]int, k)
	for i := range events {
		events[i] = next + i
	}
	out := actor.Do(Emit[int]{Events: events})
	if next+k == q.Limit && !q.Open {
		out = append(out, Done{})
	}
	return next + k, out
}

// pass passes the events of its producer on, as a producer-consumer.
type pass struct {
	From     proc.PID
	Max      int
	Dispatch Dispatcher
}

func (p pass) Dispatcher() Dispatcher { return p.Dispatch }

func (p pass) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(Subscribe{To: p.From, Max: p.Max}), nil
}

func (pass) HandleEvents(n int, events []int, _ any) (int, []actor.Effect) {
	return n + len(events), actor.Do(Emit[int]{Events: events})
}

// collect keeps the events of its producer, going on once it is done.
type collect struct {
	From      proc.PID
	Max, Min  int
	Partition int
}

func (c collect) Init(proc.PID) ([]int, []actor.Effect, error) {
	sub := Subscribe{To: c.From, Max: c.Max, Min: c.Min, Partition: c.Partition, Ending: Transient}
	return nil, actor.Do(sub), nil
}

func (collect) HandleEvents(got []int, events []int, _ any) ([]int, []actor.Effect) {
	return append(slices.Clip(got), events...), nil
}

// pipeline is a pipeline chaos makes: a producer, a producer-consumer
// maybe, and consumers.
type pipeline struct {
	s         *gensim.Sim
	src, mid  proc.PID
	sinks     []proc.PID
	broadcast bool
}

const chaosLimit = 200

// newPipeline makes a pipeline as seed draws it from r: the dispatcher of
// the stage the consumers subscribe to, a producer-consumer or not, the
// demand of each.
func newPipeline(t *testing.T, r *rand.Rand, s *gensim.Sim) pipeline {
	t.Helper()
	dispatchers := []Dispatcher{Demand{}, Broadcast{}, Partition[int]{Partitions: 2, Hash: func(n int) int { return n % 2 }}}
	which := r.IntN(len(dispatchers))
	p := pipeline{s: s, broadcast: which == 1}
	src := seq{Limit: chaosLimit, Over: 1 + r.IntN(3), Dispatch: dispatchers[which]}
	if r.IntN(2) == 0 {
		src.Dispatch = nil
	}
	p.src = spawnStage(t, s, Produce(src))
	from := p.src
	if src.Dispatch == nil {
		p.mid = spawnStage(t, s, ProduceConsume(pass{From: p.src, Max: 1 + r.IntN(8), Dispatch: dispatchers[which]}))
		from = p.mid
	}
	consumers := 1 + r.IntN(3)
	if which == 2 {
		consumers = 2
	}
	for k := range consumers {
		maxDemand := 1 + r.IntN(10)
		c := collect{From: from, Max: maxDemand, Min: r.IntN(maxDemand), Partition: k}
		p.sinks = append(p.sinks, spawnStage(t, s, Consume(c)))
	}
	return p
}

func spawnStage[S any](t *testing.T, s *gensim.Sim, st Stage[S]) proc.PID {
	t.Helper()
	pid, err := gensim.Spawn(s, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// chaos runs a pipeline drawn from seed, killing a consumer of it maybe,
// and checks it: no producer sends more than asked for, each consumer
// gets its events in order, none twice unless broadcast; with no consumer
// killed, all are through, and the pipeline ends.
func chaos(t *testing.T, seed uint64, opts ...gensim.Option) {
	t.Helper()
	r := rand.New(rand.NewPCG(seed, 7))
	s := gensim.New(seed, opts...)
	p := newPipeline(t, r, s)
	killed := len(p.sinks) > 1 && r.IntN(2) == 0
	if killed {
		for range r.IntN(300) {
			s.Step()
		}
		s.Exit(p.sinks[r.IntN(len(p.sinks))], proc.Kill)
	}
	s.RunUntilIdle()

	subscriptionsSent(t, s)
	var all []int
	for _, pid := range p.sinks {
		got, ok := gensim.State[ConsumerState[[]int]](s, pid)
		if !ok {
			continue // killed
		}
		events := got.State()
		if !slices.IsSorted(events) || len(slices.Compact(slices.Clone(events))) != len(events) {
			t.Fatalf("seed %d: %v got %v", seed, pid, events)
		}
		all = append(all, events...)
	}
	if killed {
		return
	}
	p.whole(t, seed, all)
}

// whole checks that all events are through the pipeline, and its
// producers have ended.
func (p pipeline) whole(t *testing.T, seed uint64, all []int) {
	t.Helper()
	slices.Sort(all)
	if p.broadcast {
		all = slices.Compact(all)
	}
	if !slices.Equal(all, upTo(chaosLimit)) {
		t.Fatalf("seed %d: %d events through, %v", seed, len(all), all)
	}
	for _, pid := range []proc.PID{p.src, p.mid} {
		if pid.IsZero() {
			continue
		}
		if reason, ok := exitOf(p.s, pid); !ok || !errors.Is(reason, proc.Normal) {
			t.Fatalf("seed %d: %v exited %v, with %v", seed, pid, ok, reason)
		}
	}
}

func upTo(n int) []int {
	ns := make([]int, n)
	for i := range ns {
		ns[i] = i
	}
	return ns
}

func exitOf(s *gensim.Sim, pid proc.PID) (error, bool) {
	for _, e := range s.Trace() {
		if e.Kind == gensim.Exited && e.To == pid {
			err, _ := e.Msg.(error)
			return err, true
		}
	}
	return nil, false
}

func TestChaos(t *testing.T) {
	for seed := range uint64(300) {
		chaos(t, seed)
	}
}

// FuzzChaos is TestChaos with the order of events searched by the fuzzer,
// the pipeline still that of the seed.
func FuzzChaos(f *testing.F) {
	f.Add(uint64(0), []byte{})
	f.Add(uint64(5), []byte{3, 1, 4, 1, 5, 9, 2, 6})
	f.Fuzz(func(t *testing.T, seed uint64, schedule []byte) {
		chaos(t, seed, gensim.WithChoices(schedule))
	})
}
