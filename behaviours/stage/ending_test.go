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

// TestEnding has the producer of a consumer end, normally or not: the
// consumer is told, and stops or goes on as the Ending says.
func TestEnding(t *testing.T) {
	cases := []struct {
		ending stage.Ending
		reason error
		stops  bool
	}{
		{stage.Permanent, nil, true},
		{stage.Permanent, errBoom, true},
		{stage.Transient, nil, false},
		{stage.Transient, errBoom, true},
		{stage.Temporary, nil, false},
		{stage.Temporary, errBoom, false},
	}
	for _, c := range cases {
		s := gensim.New(1)
		src := spawn(t, s, stage.Produce(counter{Limit: 30}))
		dst := spawn(t, s, stage.Consume(sink{From: src, Ending: c.ending}))
		s.RunUntilIdle()
		s.Send(src, actor.Do(actor.Stop{Reason: c.reason}))
		s.RunUntilIdle()

		if s.Alive(dst) == c.stops {
			t.Fatalf("ending %d, %v: alive %v", c.ending, c.reason, s.Alive(dst))
		}
		if c.stops {
			continue
		}
		g := gotOf(t, s, dst)
		want := c.reason
		if want == nil {
			want = proc.Normal
		}
		if len(g.ends) != 1 || !errors.Is(g.ends[0], want) || !slices.Equal(g.events, upTo(30)) {
			t.Fatalf("ending %d, %v: ends %v, %d events", c.ending, c.reason, g.ends, len(g.events))
		}
	}
}

// TestEndingCancelled has a producer cancel a consumer, which goes on.
func TestEndingCancelled(t *testing.T) {
	s := gensim.New(1)
	part := stage.Partition[int]{Partitions: 1, Hash: mod(1)}
	src := spawn(t, s, stage.Produce(counter{Limit: 30, Dispatch: part}))
	spawn(t, s, stage.Consume(sink{From: src}))
	s.RunUntilIdle()
	dst := spawn(t, s, stage.Consume(sink{From: src, Ending: stage.Temporary}))
	s.RunUntilIdle()

	if !s.Alive(dst) {
		t.Fatal("the consumer cancelled is gone")
	}
	if g := gotOf(t, s, dst); len(g.ends) != 1 || !errors.Is(g.ends[0], stage.ErrPartitionTaken) {
		t.Fatalf("ends %v", g.ends)
	}
}

// merge takes the events of two producers, as a producer-consumer, going
// on when one is done.
type merge struct {
	From []actor.Dest
}

func (m merge) Init(proc.PID) (int, []actor.Effect, error) {
	var out []actor.Effect
	for i, from := range m.From {
		out = append(out, stage.Subscribe{To: from, Tag: i, Max: 5, Ending: stage.Transient})
	}
	return 0, out, nil
}

func (merge) HandleEvents(n int, events []int, _ any) (int, []actor.Effect) {
	return n + len(events), actor.Do(stage.Emit[int]{Events: events})
}

// TestTransientMerge has a producer-consumer merge two producers, one
// done early: it goes on with the other, and the consumer gets all of
// both.
func TestTransientMerge(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		short := spawn(t, s, stage.Produce(counter{Limit: 10, Done: true}))
		long := spawn(t, s, stage.Produce(counter{Limit: 100}))
		mid := spawn(t, s, stage.ProduceConsume(merge{From: []actor.Dest{short, long}}))
		dst := spawn(t, s, stage.Consume(sink{From: mid, Max: 4}))
		s.RunUntilIdle()

		if !s.Alive(mid) || s.Alive(short) {
			t.Fatalf("seed %d: the merge alive %v, the short one %v", seed, s.Alive(mid), s.Alive(short))
		}
		g := gotOf(t, s, dst)
		all := slices.Sorted(slices.Values(g.events))
		want := slices.Sorted(slices.Values(append(upTo(10), upTo(100)...)))
		if !slices.Equal(all, want) {
			t.Fatalf("seed %d: got %v", seed, g.events)
		}
	}
}

// mergeAll takes the events of its producers, Transient, and is done once
// they all are: it stops when it has handled what it holds of them.
type mergeAll struct {
	From []actor.Dest
}

func (m mergeAll) Init(proc.PID) (int, []actor.Effect, error) {
	var out []actor.Effect
	for i, from := range m.From {
		out = append(out, stage.Subscribe{To: from, Tag: i, Max: 50, Ending: stage.Transient})
	}
	return 0, out, nil
}

func (mergeAll) HandleEvents(ended int, events []int, _ any) (int, []actor.Effect) {
	return ended, actor.Do(stage.Emit[int]{Events: events})
}

func (m mergeAll) HandleCancel(ended int, _ any, _ error) (int, []actor.Effect) {
	if ended++; ended < len(m.From) {
		return ended, nil
	}
	return ended, actor.Do(stage.Done{})
}

// TestDoneHandlesQueue has a producer-consumer done with events still
// queued, its consumer slow: it handles them all before it stops.
func TestDoneHandlesQueue(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		a := spawn(t, s, stage.Produce(counter{Limit: 100, Done: true}))
		b := spawn(t, s, stage.Produce(counter{Limit: 100, Done: true}))
		mid := spawn(t, s, stage.ProduceConsume(mergeAll{From: []actor.Dest{a, b}}))
		dst := spawn(t, s, stage.Consume(sink{From: mid, Max: 2, Ending: stage.Temporary}))
		s.RunUntilIdle()

		if reason, _ := exitOf(s, mid); !errors.Is(reason, proc.Normal) {
			t.Fatalf("seed %d: the merge exited with %v", seed, reason)
		}
		if g := gotOf(t, s, dst); len(g.events) != 200 {
			t.Fatalf("seed %d: %d events through", seed, len(g.events))
		}
	}
}
