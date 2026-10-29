package stage_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// TestAccumulate has a producer hold the demand of its consumers until
// Forward: then it is told of all of it.
func TestAccumulate(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 100, Hold: true}))
	a := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
	b := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
	s.RunUntilIdle()
	if p, _ := gensim.State[stage.ProducerState[int]](s, src); p.State() != 0 {
		t.Fatalf("%d events emitted before Forward", p.State())
	}

	s.Send(src, actor.Do(stage.Forward{}))
	s.RunUntilIdle()
	all := append(gotOf(t, s, a).events, gotOf(t, s, b).events...)
	slices.Sort(all)
	if !slices.Equal(all, upTo(100)) {
		t.Fatalf("got %v", all)
	}
}

// TestAccumulateFewEvents has a producer with fewer events than its
// consumers ask for: done at once once forwarded, it has all its
// consumers, none coming late to it gone.
func TestAccumulateFewEvents(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 2, Done: true, Hold: true}))
		var sinks []proc.PID
		for range 4 {
			sinks = append(sinks, spawn(t, s, stage.Consume(sink{From: src, Max: 1})))
		}
		s.Send(src, actor.Do(stage.Forward{}))
		s.RunUntilIdle()

		for _, pid := range sinks {
			if reason, _ := exitOf(s, pid); !errors.Is(reason, proc.Normal) {
				t.Fatalf("seed %d: %v exited with %v", seed, pid, reason)
			}
		}
	}
}

// TestAccumulateProducerConsumer has a producer-consumer hold the demand
// of its consumers: it handles nothing until Forward.
func TestAccumulateProducerConsumer(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 50}))
	mid := spawn(t, s, stage.ProduceConsume(relay{From: src, Max: 10, Hold: true}))
	dst := spawn(t, s, stage.Consume(sink{From: mid, Max: 10}))
	s.RunUntilIdle()
	if n := handledBy(t, s, mid); n != 0 {
		t.Fatalf("handled %d events before Forward", n)
	}

	s.Send(mid, actor.Do(stage.Forward{}))
	s.RunUntilIdle()
	if g := gotOf(t, s, dst); !slices.Equal(g.events, upTo(50)) {
		t.Fatalf("got %v", g.events)
	}
}

// TestHeld has consumers subscribe held to a producer of fewer events than
// they are: it emits nothing until they release; then each ends normally,
// none coming late to it done.
func TestHeld(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 2, Done: true}))
		var sinks []proc.PID
		for range 4 { // one after the other, the first long started as the last starts
			sinks = append(sinks, spawn(t, s, stage.Consume(sink{From: src, Max: 1, Held: true})))
			s.RunUntilIdle()
		}
		if p, ok := gensim.State[stage.ProducerState[int]](s, src); !ok || p.State() != 0 {
			t.Fatalf("seed %d: %d events emitted held, alive %v", seed, p.State(), ok)
		}
		for _, pid := range sinks {
			s.Send(pid, actor.Do(stage.Release{Tag: "src"}))
		}
		s.RunUntilIdle()
		for _, pid := range sinks {
			if reason, _ := exitOf(s, pid); !errors.Is(reason, proc.Normal) {
				t.Fatalf("seed %d: %v exited with %v", seed, pid, reason)
			}
		}
	}
}
