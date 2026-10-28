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

// pushed has a producer of buf, with no consumer, emit the integers below
// n, and then a consumer subscribe, and returns what it gets, and the
// events dropped.
func pushed(t *testing.T, buf stage.Buffer, n int) ([]int, int) {
	t.Helper()
	dropped := 0
	s := gensim.New(1, gensim.WithEvents(func(e any) {
		if d, ok := e.(stage.Dropped); ok {
			dropped += d.Events
		}
	}))
	src := spawn(t, s, stage.Produce(counter{Buf: buf}))
	s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(n)}))
	s.RunUntilIdle()
	dst := spawn(t, s, stage.Consume(sink{From: src, Max: 1000}))
	s.RunUntilIdle()
	return gotOf(t, s, dst).events, dropped
}

func TestBuffer(t *testing.T) {
	cases := []struct {
		name    string
		buf     stage.Buffer
		n       int
		want    []int
		dropped int
	}{
		{"drop oldest", stage.Buffer{Size: 10}, 30, upTo(30)[20:], 20},
		{"drop newest", stage.Buffer{Size: 10, Overflow: stage.DropNewest}, 30, upTo(10), 20},
		{"default", stage.Buffer{}, stage.DefaultBuffer + 5, upTo(stage.DefaultBuffer + 5)[5:], 5},
		{"no bound", stage.Buffer{Size: -1}, 3 * stage.DefaultBuffer, upTo(3 * stage.DefaultBuffer), 0},
	}
	for _, c := range cases {
		got, dropped := pushed(t, c.buf, c.n)
		if !slices.Equal(got, c.want) || dropped != c.dropped {
			t.Errorf("%s: got %d events from %v, dropped %d", c.name, len(got), got[:min(len(got), 3)], dropped)
		}
	}
}

func TestBufferStopOnFull(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Buf: stage.Buffer{Size: 10, Overflow: stage.StopOnFull}}))
	s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(10)}))
	s.RunUntilIdle()
	if !s.Alive(src) {
		t.Fatal("stopped with a buffer just full")
	}
	s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(1)}))
	s.RunUntilIdle()
	if reason, _ := exitOf(s, src); !errors.Is(reason, stage.ErrBufferFull) {
		t.Fatalf("exited with %v", reason)
	}
}

// TestBufferPartitions has the queues of partitions cut to the same
// level, the longest losing most.
func TestBufferPartitions(t *testing.T) {
	s := gensim.New(1)
	hash := func(n int) int {
		if n < 20 {
			return 0
		}
		return 1
	}
	part := stage.Partition[int]{Partitions: 2, Hash: hash}
	src := spawn(t, s, stage.Produce(counter{Dispatch: part, Buf: stage.Buffer{Size: 10}}))
	s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(25)})) // 20 and 5
	s.RunUntilIdle()
	a := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
	b := spawn(t, s, stage.Consume(sink{From: src, Max: 10, Partition: 1}))
	s.RunUntilIdle()

	if g := gotOf(t, s, a); !slices.Equal(g.events, upTo(20)[15:]) {
		t.Errorf("partition 0 got %v", g.events)
	}
	if g := gotOf(t, s, b); !slices.Equal(g.events, upTo(25)[20:]) {
		t.Errorf("partition 1 got %v", g.events)
	}
}

// TestDone has a producer done stop once its consumers have all it held:
// a Stop would lose them.
func TestDone(t *testing.T) {
	dispatchers := map[string]stage.Dispatcher{
		"demand":    stage.Demand{},
		"broadcast": stage.Broadcast{},
	}
	for name, d := range dispatchers {
		for seed := range uint64(20) {
			s := gensim.New(seed)
			src := spawn(t, s, stage.Produce(counter{Limit: 100, Over: 3, Done: true, Dispatch: d}))
			dst := spawn(t, s, stage.Consume(sink{From: src, Max: 8}))
			var g got
			for s.Alive(dst) {
				g = gotOf(t, s, dst)
				if !s.Step() {
					t.Fatalf("%s seed %d: idle with %d events", name, seed, len(g.events))
				}
			}
			for _, pid := range []proc.PID{src, dst} {
				if reason, _ := exitOf(s, pid); !errors.Is(reason, proc.Normal) {
					t.Fatalf("%s seed %d: %v exited with %v", name, seed, pid, reason)
				}
			}
			if !slices.Equal(g.events, upTo(100)) {
				t.Fatalf("%s seed %d: got %v", name, seed, g.events)
			}
		}
	}
}
