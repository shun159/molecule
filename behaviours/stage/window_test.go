package stage_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// tally counts the events of its producer by window of a second, as a
// consumer, keeping "second:count" for each window.
type tally struct{ From actor.Dest }

type tallied struct {
	count   int
	windows []string
}

func (t tally) Init(proc.PID) (tallied, []actor.Effect, error) {
	return tallied{}, actor.Do(stage.Subscribe{To: t.From, Max: 100}), nil
}

func (tally) HandleEvents(t tallied, events []int, _ any) (tallied, []actor.Effect) {
	t.count += len(events)
	return t, nil
}

func (tally) Window() time.Duration { return time.Second }

func (tally) HandleWindow(t tallied, start time.Time) (tallied, []actor.Effect) {
	t.windows = append(slices.Clip(t.windows), fmt.Sprintf("%d:%d", start.Second(), t.count))
	t.count = 0
	return t, nil
}

// push has src emit n events.
func push(s *gensim.Sim, src proc.PID, n int) {
	s.Send(src, actor.Do(stage.Emit[int]{Events: upTo(n)}))
	s.RunUntilIdle()
}

// TestWindow has events come at times: each window of a second is told
// of with those it had, none for one with none.
func TestWindow(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{}))
	dst := spawn(t, s, stage.Consume(tally{From: src}))
	s.RunUntilIdle()
	push(s, src, 5) // at 0s
	s.Advance(1200 * time.Millisecond)
	push(s, src, 3) // at 1.2s
	s.Advance(2 * time.Second)
	push(s, src, 2) // at 3.2s
	s.Advance(time.Second)

	c, _ := gensim.State[stage.ConsumerState[tallied]](s, dst)
	if want := []string{"0:5", "1:3", "2:0", "3:2"}; !slices.Equal(c.State().windows, want) {
		t.Fatalf("windows %v, want %v", c.State().windows, want)
	}
}

// perWindow counts the events of its producer by window of a second, as
// a producer-consumer, and emits the count of each.
type perWindow struct{ From actor.Dest }

func (p perWindow) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: p.From, Max: 100}), nil
}

func (perWindow) HandleEvents(n int, events []int, _ any) (int, []actor.Effect) {
	return n + len(events), nil
}

func (perWindow) Window() time.Duration { return time.Second }

func (perWindow) HandleWindow(n int, _ time.Time) (int, []actor.Effect) {
	return 0, actor.Do(stage.Emit[int]{Events: []int{n}})
}

// TestWindowEmits has a producer-consumer emit what it counted in each
// window, down a pipeline.
func TestWindowEmits(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{}))
	mid := spawn(t, s, stage.ProduceConsume(perWindow{From: src}))
	dst := spawn(t, s, stage.Consume(sink{From: mid}))
	s.RunUntilIdle()
	push(s, src, 7)
	s.Advance(1500 * time.Millisecond)
	push(s, src, 4)
	s.Advance(time.Second)

	if g := gotOf(t, s, dst); !slices.Equal(g.events, []int{7, 4}) {
		t.Fatalf("got %v", g.events)
	}
}

// TestWindowAligned has a stage start within a second: its first window
// ends at the second, not a second after it started.
func TestWindowAligned(t *testing.T) {
	s := gensim.New(1)
	src := spawn(t, s, stage.Produce(counter{}))
	s.Advance(300 * time.Millisecond)
	dst := spawn(t, s, stage.Consume(tally{From: src}))
	s.RunUntilIdle()
	push(s, src, 2)
	s.Advance(800 * time.Millisecond) // to 1.1s

	c, _ := gensim.State[stage.ConsumerState[tallied]](s, dst)
	if want := []string{"0:2"}; !slices.Equal(c.State().windows, want) {
		t.Fatalf("windows %v at 1.1s", c.State().windows)
	}
}
