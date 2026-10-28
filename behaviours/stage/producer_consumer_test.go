package stage_test

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// relay keeps the integers of its producer that Every divides, all if
// zero, emitting Copies of each, one if zero. Its state is how many it
// has handled.
type relay struct {
	From     actor.Dest
	Max, Min int
	Every    int
	Copies   int
	Dispatch stage.Dispatcher
	Hold     bool
}

func (r relay) Dispatcher() stage.Dispatcher { return r.Dispatch }

func (r relay) Accumulate() bool { return r.Hold }

func (r relay) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: r.From, Max: r.Max, Min: r.Min}), nil
}

func (r relay) HandleEvents(handled int, events []int, _ any) (int, []actor.Effect) {
	var out []int
	for _, n := range events {
		if r.keeps(n) {
			for range max(r.Copies, 1) {
				out = append(out, n)
			}
		}
	}
	return handled + len(events), actor.Do(stage.Emit[int]{Events: out})
}

// HandleInfo returns the effects it is sent.
func (relay) HandleInfo(handled int, msg any) (int, []actor.Effect) {
	effs, _ := msg.([]actor.Effect)
	return handled, effs
}

func (r relay) keeps(n int) bool { return r.Every == 0 || n%r.Every == 0 }

// relayed is what the sink of a relay of the integers below limit gets.
func (r relay) relayed(limit int) []int {
	var want []int
	for n := range limit {
		if r.keeps(n) {
			for range max(r.Copies, 1) {
				want = append(want, n)
			}
		}
	}
	return want
}

func TestPipeline(t *testing.T) {
	cases := []struct {
		over   int
		relay  relay
		sinkMx int
	}{
		{over: 1, relay: relay{Max: 10, Min: 5}, sinkMx: 10},
		{over: 3, relay: relay{Max: 10, Min: 5, Every: 3}, sinkMx: 4},
		{over: 1, relay: relay{Max: 4, Min: 1, Copies: 3}, sinkMx: 10},
		{over: 2, relay: relay{Max: 1, Every: 5, Copies: 2}, sinkMx: 1},
		{over: 1, relay: relay{Max: 50, Every: 100}, sinkMx: 2}, // the sink asks less than it gets
	}
	for _, c := range cases {
		for seed := range uint64(20) {
			s := gensim.New(seed)
			src := spawn(t, s, stage.Produce(counter{Limit: 200, Over: c.over}))
			r := c.relay
			r.From = src
			mid := spawn(t, s, stage.ProduceConsume(r))
			dst := spawn(t, s, stage.Consume(sink{From: mid, Max: c.sinkMx}))
			s.RunUntilIdle()

			if g := gotOf(t, s, dst); !slices.Equal(g.events, r.relayed(200)) {
				t.Fatalf("%+v seed %d: got %v", c, seed, g.events)
			}
		}
	}
}

// TestPipelineFanOut has a producer-consumer feed two consumers.
func TestPipelineFanOut(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 200}))
		r := relay{From: src, Max: 6, Every: 2, Copies: 2}
		mid := spawn(t, s, stage.ProduceConsume(r))
		a := spawn(t, s, stage.Consume(sink{From: mid, Max: 3}))
		b := spawn(t, s, stage.Consume(sink{From: mid, Max: 7}))
		s.RunUntilIdle()

		all := append(gotOf(t, s, a).events, gotOf(t, s, b).events...)
		slices.Sort(all)
		if !slices.Equal(all, r.relayed(200)) {
			t.Fatalf("seed %d: got %v together", seed, all)
		}
	}
}

// TestPipelineEnds has a pipeline end once all is through it: the
// producer done, the producer-consumer stops once it has sent all.
func TestPipelineEnds(t *testing.T) {
	for seed := range uint64(30) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 60, Stop: true}))
		r := relay{From: src, Max: 8, Copies: 3}
		mid := spawn(t, s, stage.ProduceConsume(r))
		dst := spawn(t, s, stage.Consume(sink{From: mid, Max: 5}))
		var g got
		for s.Alive(dst) {
			g = gotOf(t, s, dst)
			if !s.Step() {
				t.Fatalf("seed %d: idle, the sink alive with %d events", seed, len(g.events))
			}
		}
		for _, pid := range []proc.PID{mid, dst} {
			if reason, _ := exitOf(s, pid); !errors.Is(reason, proc.Normal) {
				t.Fatalf("seed %d: %v exited with %v", seed, pid, reason)
			}
		}
		if !slices.Equal(g.events, r.relayed(60)) {
			t.Fatalf("seed %d: got %v", seed, g.events)
		}
	}
}

func TestPipelineCrash(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 10}))
	mid := spawn(t, s, stage.ProduceConsume(relay{From: src}))
	dst := spawn(t, s, stage.Consume(sink{From: mid}))
	s.RunUntilIdle()
	s.Send(src, actor.Do(actor.Stop{Reason: errBoom}))
	s.RunUntilIdle()
	for _, pid := range []proc.PID{mid, dst} {
		if reason, _ := exitOf(s, pid); !errors.Is(reason, errBoom) {
			t.Fatalf("%v exited with %v", pid, reason)
		}
	}
}

// TestNoDemandDownstream has a producer-consumer without a consumer
// handle nothing, and hold no more than it asked for.
func TestNoDemandDownstream(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 1000}))
	mid := spawn(t, s, stage.ProduceConsume(relay{From: src, Max: 10}))
	s.RunUntilIdle()
	if n := handledBy(t, s, mid); n != 0 {
		t.Fatalf("handled %d events", n)
	}
	next, _ := gensim.State[stage.ProducerState[int]](s, src)
	if next.State() != 10 {
		t.Fatalf("the producer emitted %d events", next.State())
	}

	dst := spawn(t, s, stage.Consume(sink{From: mid, Max: 10}))
	s.RunUntilIdle()
	if g := gotOf(t, s, dst); !slices.Equal(g.events, upTo(1000)) {
		t.Fatalf("got %d events", len(g.events))
	}
}

func handledBy(t *testing.T, s *gensim.Sim, pid proc.PID) int {
	t.Helper()
	p, ok := gensim.State[stage.ProducerConsumerState[int, int]](s, pid)
	if !ok {
		t.Fatalf("%v is gone", pid)
	}
	return p.State()
}

func ExampleProduceConsume() {
	s := gensim.New(1)
	src, _ := gensim.Spawn(s, stage.Produce(counter{Limit: 10}), nil)
	evens, _ := gensim.Spawn(s, stage.ProduceConsume(relay{From: src, Every: 2}), nil)
	dst, _ := gensim.Spawn(s, stage.Consume(sink{From: evens}), nil)
	s.RunUntilIdle()
	c, _ := gensim.State[stage.ConsumerState[got]](s, dst)
	fmt.Println(c.State().events)
	// Output: [0 2 4 6 8]
}
