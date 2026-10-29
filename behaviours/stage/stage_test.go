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

// counter emits the integers from 0 to Limit-1, Over times as many as
// asked for at once, the rest waiting in its buffer, and stops once done,
// if Stop. Its state is the next integer.
type counter struct {
	Limit int
	Over  int
	Stop  bool
}

func (counter) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (c counter) HandleDemand(next, n int) (int, []actor.Effect) {
	k := min(n*max(c.Over, 1), c.Limit-next)
	events := make([]int, k)
	for i := range events {
		events[i] = next + i
	}
	next += k
	out := actor.Do(stage.Emit[int]{Events: events})
	if c.Stop && next == c.Limit {
		out = append(out, actor.Stop{})
	}
	return next, out
}

// HandleInfo returns the effects it is sent.
func (counter) HandleInfo(next int, msg any) (int, []actor.Effect) {
	effs, _ := msg.([]actor.Effect)
	return next, effs
}

// sink keeps the events of its producer, and the size of each batch.
type sink struct {
	From     actor.Dest
	Max, Min int
}

type got struct {
	events  []int
	batches []int
}

func (s sink) Init(proc.PID) (got, []actor.Effect, error) {
	return got{}, actor.Do(stage.Subscribe{To: s.From, Tag: "src", Max: s.Max, Min: s.Min}), nil
}

func (sink) HandleEvents(g got, events []int, tag any) (got, []actor.Effect) {
	if tag != "src" {
		panic(fmt.Sprint("tag ", tag))
	}
	g.events = append(slices.Clip(g.events), events...)
	g.batches = append(slices.Clip(g.batches), len(events))
	return g, nil
}

// HandleInfo returns the effects it is sent.
func (sink) HandleInfo(g got, msg any) (got, []actor.Effect) {
	effs, _ := msg.([]actor.Effect)
	return g, effs
}

func spawn[S any](t *testing.T, s *gensim.Sim, st stage.Stage[S]) proc.PID {
	t.Helper()
	pid, err := gensim.Spawn(s, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func gotOf(t *testing.T, s *gensim.Sim, pid proc.PID) got {
	t.Helper()
	c, ok := gensim.State[stage.ConsumerState[got]](s, pid)
	if !ok {
		t.Fatalf("%v is gone", pid)
	}
	return c.State()
}

// exitOf returns the reason pid exited with, and whether it did.
func exitOf(s *gensim.Sim, pid proc.PID) (error, bool) {
	for _, e := range s.Trace() {
		if e.Kind == gensim.Exited && e.To == pid {
			err, _ := e.Msg.(error)
			return err, true
		}
	}
	return nil, false
}

func upTo(n int) []int {
	ns := make([]int, n)
	for i := range ns {
		ns[i] = i
	}
	return ns
}

func TestDelivery(t *testing.T) {
	cases := []struct {
		over, max, min int
	}{
		{over: 1, max: 10, min: 5},
		{over: 3, max: 10, min: 5},
		{over: 1, max: 1},
		{over: 7, max: 4, min: 1},
		{over: 1}, // the defaults
	}
	for _, c := range cases {
		for seed := range uint64(20) {
			s := gensim.New(seed)
			src := spawn(t, s, stage.Produce(counter{Limit: 100, Over: c.over}))
			dst := spawn(t, s, stage.Consume(sink{From: src, Max: c.max, Min: c.min}))
			s.RunUntilIdle()

			g := gotOf(t, s, dst)
			if !slices.Equal(g.events, upTo(100)) {
				t.Fatalf("%+v seed %d: got %v", c, seed, g.events)
			}
			maxDemand, minDemand := c.max, c.min
			if maxDemand == 0 {
				maxDemand = stage.DefaultMax
			}
			if minDemand == 0 {
				minDemand = maxDemand * 3 / 4
			}
			if big := slices.Max(g.batches); big > maxDemand-minDemand {
				t.Fatalf("%+v seed %d: a batch of %d", c, seed, big)
			}
		}
	}
}

func TestProducerDone(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 50, Over: 2, Stop: true}))
		dst := spawn(t, s, stage.Consume(sink{From: src, Max: 8}))
		var g got
		s.Step()
		for s.Alive(dst) {
			g = gotOf(t, s, dst)
			s.Step()
		}
		if reason, _ := exitOf(s, dst); !errors.Is(reason, proc.Normal) {
			t.Fatalf("seed %d: the consumer exited with %v", seed, reason)
		}
		if !slices.Equal(g.events, upTo(50)) {
			t.Fatalf("seed %d: got %v", seed, g.events)
		}
	}
}

var errBoom = errors.New("boom")

func TestProducerCrash(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 10}))
	dst := spawn(t, s, stage.Consume(sink{From: src}))
	s.RunUntilIdle()
	s.Send(src, actor.Do(actor.Stop{Reason: errBoom}))
	s.RunUntilIdle()
	if reason, _ := exitOf(s, dst); !errors.Is(reason, errBoom) {
		t.Fatalf("the consumer exited with %v", reason)
	}
}

func TestNoProducer(t *testing.T) {
	s := gensim.New(1)
	dst := spawn(t, s, stage.Consume(sink{From: actor.Local("none")}))
	s.RunUntilIdle()
	if reason, _ := exitOf(s, dst); !errors.Is(reason, proc.NoProc) {
		t.Fatalf("the consumer exited with %v", reason)
	}
}

// TestManyConsumers has consumers share the events of a producer: each
// event goes to one of them, in order for each.
func TestManyConsumers(t *testing.T) {
	for _, over := range []int{1, 3} {
		for seed := range uint64(20) {
			s := gensim.New(seed)
			src := spawn(t, s, stage.Produce(counter{Limit: 300, Over: over}))
			var sinks []proc.PID
			for _, maxDemand := range []int{10, 4, 25} {
				sinks = append(sinks, spawn(t, s, stage.Consume(sink{From: src, Max: maxDemand})))
			}
			s.RunUntilIdle()

			var all []int
			for _, pid := range sinks {
				g := gotOf(t, s, pid)
				if len(g.events) == 0 || !slices.IsSorted(g.events) {
					t.Fatalf("over %d seed %d: %v got %v", over, seed, pid, g.events)
				}
				all = append(all, g.events...)
			}
			slices.Sort(all)
			if !slices.Equal(all, upTo(300)) {
				t.Fatalf("over %d seed %d: got %v together", over, seed, all)
			}
		}
	}
}

// TestConsumerLeaves has the others go on when a consumer leaves.
func TestConsumerLeaves(t *testing.T) {
	for seed := range uint64(20) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{Limit: 300, Over: 2}))
		first := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
		other := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
		for len(gotOf(t, s, first).events) == 0 {
			s.Step()
		}
		s.Exit(first, proc.Kill)
		s.RunUntilIdle()

		// What was sent the first and not handled is lost with it.
		g := gotOf(t, s, other)
		if !slices.IsSorted(g.events) || g.events[len(g.events)-1] != 299 {
			t.Fatalf("seed %d: the other got %v", seed, g.events)
		}
	}
}

// TestDemandOfConsumerGone has a consumer leave while owed events: they
// go to the other, its own demand met first, the rest as it asks.
func TestDemandOfConsumerGone(t *testing.T) {
	for seed := range uint64(10) {
		s := gensim.New(seed)
		src := spawn(t, s, stage.Produce(counter{})) // emits only what it is sent
		gone := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
		other := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
		s.RunUntilIdle()
		s.Exit(gone, proc.Kill)
		s.RunUntilIdle()
		s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(30)}))
		s.RunUntilIdle()

		if g := gotOf(t, s, other); !slices.Equal(g.events, upTo(30)) {
			t.Fatalf("seed %d: the other got %v", seed, g.events)
		}
	}
}

// TestNextConsumer has a consumer take over from one gone: the events
// buffered for the first go to the next.
func TestNextConsumer(t *testing.T) {
	ends := map[string]func(*gensim.Sim, proc.PID){
		"cancel": func(s *gensim.Sim, pid proc.PID) {
			s.Send(pid, actor.Do(stage.Cancel{Tag: "src"}))
		},
		"exit": func(s *gensim.Sim, pid proc.PID) { s.Exit(pid, proc.Kill) },
	}
	for name, end := range ends {
		for seed := range uint64(10) {
			s := gensim.New(seed)
			src := spawn(t, s, stage.Produce(counter{Limit: 100, Over: 3}))
			first := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
			for len(gotOf(t, s, first).events) == 0 {
				s.Step()
			}
			end(s, first)
			s.RunUntilIdle()
			next := spawn(t, s, stage.Consume(sink{From: src, Max: 10}))
			s.RunUntilIdle()

			// What was sent the first and not handled is lost with it.
			g := gotOf(t, s, next)
			if len(g.events) == 0 || !slices.Equal(g.events, upTo(100)[g.events[0]:]) {
				t.Fatalf("%s seed %d: the next got %v", name, seed, g.events)
			}
		}
	}
}

func TestBadDemand(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 10}))
	_, err := gensim.Spawn(s, stage.Consume(sink{From: src, Max: 4, Min: 4}), nil)
	if !errors.Is(err, stage.ErrBadDemand) {
		t.Fatalf("the consumer started with %v", err)
	}
}

func TestBadEffect(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 10}))
	dst := spawn(t, s, stage.Consume(sink{From: src}))
	s.RunUntilIdle()
	s.Send(src, actor.Do(stage.Emit[string]{Events: []string{"x"}}))
	s.Send(dst, actor.Do(stage.Emit[int]{Events: []int{1}}))
	s.RunUntilIdle()
	for _, pid := range []proc.PID{src, dst} {
		reason, _ := exitOf(s, pid)
		var bad *stage.BadEffectError
		if !errors.As(reason, &bad) {
			t.Fatalf("%v exited with %v", pid, reason)
		}
	}
}

// words takes strings.
type words struct{ From actor.Dest }

func (w words) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: w.From}), nil
}

func (words) HandleEvents(n int, events []string, _ any) (int, []actor.Effect) {
	return n + len(events), nil
}

func TestBadEvents(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{Limit: 10}))
	dst := spawn(t, s, stage.Consume(words{From: src}))
	s.RunUntilIdle()
	reason, _ := exitOf(s, dst)
	var bad *stage.BadEventsError
	if !errors.As(reason, &bad) {
		t.Fatalf("the consumer exited with %v", reason)
	}
}
