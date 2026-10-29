package stage_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// resubscriber is a sink subscribing to "src" by name, again whenever
// its producer goes.
func resubscriber() stage.Stage[stage.ConsumerState[got]] {
	return stage.Consume(sink{From: actor.Local("src"), Max: 10, Ending: stage.Resubscribe})
}

// TestResubscribe has a producer go, and another come under its name:
// the consumer, told, gets the events of the next.
func TestResubscribe(t *testing.T) {
	s := gensim.New(1)
	first, _ := gensim.Spawn(s, stage.Produce(counter{Limit: 50}), nil, gensim.Named("src"))
	dst := spawn(t, s, resubscriber())
	s.RunUntilIdle()
	s.Exit(first, proc.Kill)
	s.RunUntilIdle()
	if !s.Alive(dst) {
		t.Fatal("the consumer is gone with its producer")
	}
	gensim.Spawn(s, stage.Produce(counter{Limit: 50}), nil, gensim.Named("src"))
	s.Advance(150 * time.Millisecond)

	g := gotOf(t, s, dst)
	if want := append(upTo(50), upTo(50)...); !slices.Equal(g.events, want) {
		t.Fatalf("got %v", g.events)
	}
	if len(g.ends) != 1 || !errors.Is(g.ends[0], proc.Killed) {
		t.Fatalf("ends %v", g.ends)
	}
}

// TestResubscribeBackoff has a consumer try a producer never there:
// twice as long between tries each time, up to 5s.
func TestResubscribeBackoff(t *testing.T) {
	s := gensim.New(1)
	dst := spawn(t, s, resubscriber())
	s.RunUntilIdle()
	tries := func() int { return len(gotOf(t, s, dst).ends) }
	if n := tries(); n != 1 {
		t.Fatalf("%d tries at once", n)
	}
	s.Advance(time.Second) // 0.1, 0.3, 0.7
	if n := tries(); n != 4 {
		t.Fatalf("%d tries in 1s", n)
	}
	s.Advance(11 * time.Second) // 1.5, 3.1, 6.3, 11.3: 5s at most
	if n := tries(); n != 8 {
		t.Fatalf("%d tries in 12s", n)
	}
}

// TestResubscribeReset has a consumer get events: the next time its
// producer goes, it waits 100ms again, not as long as it last waited.
func TestResubscribeReset(t *testing.T) {
	s := gensim.New(1)
	dst := spawn(t, s, resubscriber())
	s.Advance(time.Second) // tried at 0, 0.1, 0.3, 0.7; next at 1.5
	src, _ := gensim.Spawn(s, stage.Produce(counter{Limit: 10}), nil, gensim.Named("src"))
	s.Advance(600 * time.Millisecond)
	if n := len(gotOf(t, s, dst).events); n != 10 {
		t.Fatalf("%d events at 1.6s", n)
	}
	s.Exit(src, proc.Kill)
	s.RunUntilIdle()
	gensim.Spawn(s, stage.Produce(counter{Limit: 10}), nil, gensim.Named("src"))
	s.Advance(150 * time.Millisecond)
	if n := len(gotOf(t, s, dst).events); n != 20 {
		t.Fatalf("%d events 150ms after its producer went", n)
	}
}

// TestResubscribeCancel has a consumer cancel a subscription waiting to
// subscribe again: it tries no more.
func TestResubscribeCancel(t *testing.T) {
	s := gensim.New(1)
	dst := spawn(t, s, resubscriber())
	s.RunUntilIdle()
	s.Send(dst, actor.Do(stage.Cancel{Tag: "src"}))
	s.Advance(10 * time.Second)
	if n := len(gotOf(t, s, dst).ends); n != 1 {
		t.Fatalf("%d tries", n)
	}
}

// back is a consumer telling Done when events come again after its
// producer ended the subscription.
type back struct {
	From actor.Dest
	Done actor.From
}

func (b back) Init(proc.PID) (bool, []actor.Effect, error) {
	return false, actor.Do(stage.Subscribe{To: b.From, Max: 100, Ending: stage.Resubscribe}), nil
}

func (b back) HandleEvents(ended bool, _ []int, _ any) (bool, []actor.Effect) {
	if !ended {
		return false, nil
	}
	return false, actor.Do(actor.Reply{To: b.Done, Value: true})
}

func (back) HandleCancel(_ bool, _ any, _ error) (bool, []actor.Effect) { return true, nil }

// TestDistResubscribe has the connection between the nodes of a producer
// and its consumer lost: the consumer subscribes again, by name, once
// dist connects them again.
func TestDistResubscribe(t *testing.T) {
	nodes, dists := cluster(t, "a@stage", "b@stage")
	a, b := nodes[0], nodes[1]
	ctx := context.Background()
	if _, err := stage.Start(ctx, a, stage.Produce(counter{Limit: 1 << 62}), actor.WithName(actor.Local("src"))); err != nil {
		t.Fatal(err)
	}
	done := b.Alias()
	defer done.Release()
	src := actor.Remote{Node: "a@stage", Name: "src"}
	if _, err := stage.Start(ctx, b, stage.Consume(back{From: src, Done: actor.From{Tag: done.Ref}})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	dists[1].Disconnect("a@stage")
	select {
	case <-done.C:
	case <-time.After(5 * time.Second):
		t.Fatal("no events since the connection was lost")
	}
}
