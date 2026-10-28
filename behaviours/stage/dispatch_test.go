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

func TestBroadcast(t *testing.T) {
	for _, over := range []int{1, 3} {
		for seed := range uint64(20) {
			s := gensim.New(seed)
			src := spawn(t, s, stage.Produce(counter{Limit: 200, Over: over, Dispatch: stage.Broadcast{}}))
			var sinks []proc.PID
			for _, maxDemand := range []int{10, 4, 25} {
				sinks = append(sinks, spawn(t, s, stage.Consume(sink{From: src, Max: maxDemand})))
			}
			s.RunUntilIdle()

			// Each gets all from when it subscribed, and one all.
			whole := false
			for _, pid := range sinks {
				g := gotOf(t, s, pid)
				if !isTail(g.events, 200) {
					t.Fatalf("over %d seed %d: %v got %v", over, seed, pid, g.events)
				}
				whole = whole || g.events[0] == 0
			}
			if !whole {
				t.Fatalf("over %d seed %d: none got all", over, seed)
			}
		}
	}
}

// isTail reports whether ns are the integers below n from some on.
func isTail(ns []int, n int) bool {
	return len(ns) > 0 && slices.Equal(ns, upTo(n)[ns[0]:])
}

// TestBroadcastLeave has the slowest consumer leave: the others, held
// back by it, go on.
func TestBroadcastLeave(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 200, Dispatch: stage.Broadcast{}}))
		fast := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
		slow := spawn(t, s, stage.Consume(sink{From: src, Max: 2}))
		for len(gotOf(t, s, slow).events) == 0 {
			s.Step()
		}
		s.Exit(slow, proc.Kill)
		s.RunUntilIdle()

		if g := gotOf(t, s, fast); !isTail(g.events, 200) {
			t.Fatalf("seed %d: the fast one got %v", seed, g.events)
		}
	}
}

func mod(k int) func(int) int { return func(n int) int { return n % k } }

func TestPartition(t *testing.T) {
	for _, over := range []int{1, 3} {
		for seed := range uint64(20) {
			s := gensim.New(seed)
			part := stage.Partition[int]{Partitions: 3, Hash: mod(3)}
			src := spawn(t, s, stage.Produce(counter{Limit: 300, Over: over, Dispatch: part}))
			var sinks []proc.PID
			for k, maxDemand := range []int{10, 4, 25} {
				sinks = append(sinks, spawn(t, s, stage.Consume(sink{From: src, Max: maxDemand, Partition: k})))
			}
			s.RunUntilIdle()

			for k, pid := range sinks {
				var want []int
				for n := k; n < 300; n += 3 {
					want = append(want, n)
				}
				if g := gotOf(t, s, pid); !slices.Equal(g.events, want) {
					t.Fatalf("over %d seed %d: partition %d got %v", over, seed, k, g.events)
				}
			}
		}
	}
}

// TestPartitionDrops drops the events of a negative partition.
func TestPartitionDrops(t *testing.T) {
	hash := func(n int) int {
		if n%5 == 0 {
			return -1
		}
		return n % 2
	}
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 100, Dispatch: stage.Partition[int]{Partitions: 2, Hash: hash}}))
	a := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
	b := spawn(t, s, stage.Consume(sink{From: src, Max: 10, Partition: 1}))
	s.RunUntilIdle()

	all := append(gotOf(t, s, a).events, gotOf(t, s, b).events...)
	slices.Sort(all)
	want := slices.DeleteFunc(upTo(100), func(n int) bool { return n%5 == 0 })
	if !slices.Equal(all, want) {
		t.Fatalf("got %v", all)
	}
}

// TestPartitionHeldBack has a partition without a consumer hold the other
// back, holding no more events than it asked for, until one comes.
func TestPartitionHeldBack(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 2000, Dispatch: stage.Partition[int]{Partitions: 2, Hash: mod(2)}}))
	even := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
	s.RunUntilIdle()

	p, _ := gensim.State[stage.ProducerState[int]](s, src)
	if emitted := p.State(); emitted > 40 {
		t.Fatalf("%d events emitted", emitted)
	}
	odd := spawn(t, s, stage.Consume(sink{From: src, Max: 10, Partition: 1}))
	s.RunUntilIdle()
	all := append(gotOf(t, s, even).events, gotOf(t, s, odd).events...)
	slices.Sort(all)
	if !slices.Equal(all, upTo(2000)) {
		t.Fatalf("got %d events", len(all))
	}
}

func TestPartitionSubscribe(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 100, Dispatch: stage.Partition[int]{Partitions: 2, Hash: mod(2)}}))
	first := spawn(t, s, stage.Consume(sink{From: src}))
	s.RunUntilIdle()
	again := spawn(t, s, stage.Consume(sink{From: src}))
	none := spawn(t, s, stage.Consume(sink{From: src, Partition: 2}))
	s.RunUntilIdle()

	if reason, _ := exitOf(s, again); !errors.Is(reason, stage.ErrPartitionTaken) {
		t.Fatalf("a second consumer of a partition exited with %v", reason)
	}
	if reason, _ := exitOf(s, none); !errors.Is(reason, stage.ErrBadPartition) {
		t.Fatalf("a consumer of no partition exited with %v", reason)
	}
	if !s.Alive(first) {
		t.Fatal("the first consumer is gone")
	}
}

func TestPartitionBadHash(t *testing.T) {
	parts := map[string]stage.Dispatcher{
		"past the last": stage.Partition[int]{Partitions: 2, Hash: mod(3)},
		"another type":  stage.Partition[string]{Partitions: 2, Hash: func(string) int { return 0 }},
	}
	for name, part := range parts {
		s := gensim.New(1)
		src := spawn(t, s, stage.Produce(counter{Limit: 100, Dispatch: part}))
		spawn(t, s, stage.Consume(sink{From: src}))
		s.RunUntilIdle()

		reason, _ := exitOf(s, src)
		var bad *stage.BadEffectError
		if !errors.Is(reason, stage.ErrBadPartition) && !errors.As(reason, &bad) {
			t.Fatalf("%s: the producer exited with %v", name, reason)
		}
	}
}

func TestNoPartitions(t *testing.T) {
	s := gensim.New(1)
	_, err := gensim.Spawn(s, stage.Produce(counter{Dispatch: stage.Partition[int]{Hash: mod(2)}}), nil)
	if !errors.Is(err, stage.ErrNoPartitions) {
		t.Fatalf("started with %v", err)
	}
}

// TestPipelinePartition has a producer-consumer partition what it emits.
func TestPipelinePartition(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 200}))
		r := relay{From: src, Max: 6, Copies: 2, Dispatch: stage.Partition[int]{Partitions: 2, Hash: mod(2)}}
		mid := spawn(t, s, stage.ProduceConsume(r))
		even := spawn(t, s, stage.Consume(sink{From: mid, Max: 3}))
		odd := spawn(t, s, stage.Consume(sink{From: mid, Max: 7, Partition: 1}))
		s.RunUntilIdle()

		for k, pid := range []proc.PID{even, odd} {
			want := slices.DeleteFunc(r.relayed(200), func(n int) bool { return n%2 != k })
			if g := gotOf(t, s, pid); !slices.Equal(g.events, want) {
				t.Fatalf("seed %d: partition %d got %v", seed, k, g.events)
			}
		}
	}
}

// TestBroadcastPushed has a producer emit with no consumer: its events
// wait for the first.
func TestBroadcastPushed(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Dispatch: stage.Broadcast{}}))
	s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(30)}))
	s.RunUntilIdle()
	dst := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
	s.RunUntilIdle()
	if g := gotOf(t, s, dst); !slices.Equal(g.events, upTo(30)) {
		t.Fatalf("got %v", g.events)
	}
}
