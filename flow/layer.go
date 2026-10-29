package flow

import (
	"context"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// layer is a layer of a flow, its types erased: how many workers it has,
// whether it partitions what it sends on, and what starts one of them.
type layer struct {
	stages      int
	partitioned bool
	start       func(ctx context.Context, n *proc.Node, w wiring) (proc.PID, error)
}

// wiring is what a worker subscribes to, and how.
type wiring struct {
	from      []actor.Dest // the workers of the layer before, or the sources
	partition int          // of each of from, partitioned
	max       int
}

// layerOf ends the layer of f, its workers sending on as out says,
// stage.Demand if nil.
func layerOf[S, T any](f Flow[S, T], out stage.Dispatcher) layer {
	return layer{stages: f.stages, partitioned: out != nil, start: func(ctx context.Context, n *proc.Node, w wiring) (proc.PID, error) {
		return stage.Start(ctx, n, stage.ProduceConsume(worker[S, T]{wiring: w, out: out, xf: f.xf, red: f.red}))
	}}
}

// worker runs the operations of a layer on the events it subscribes to,
// or the reduce. It subscribes to all, Transient and held, until Run,
// having started all the workers, releases them: no worker comes late to
// another, or to a source, done before it subscribed. It is done once all
// it subscribed to are, having let out what it reduced; its consumer
// gone, it stops.
type worker[S, T any] struct {
	wiring
	out stage.Dispatcher
	xf  func(emit func(T)) func(S)
	red *reducer[S, T]
	now func() time.Time // the clock of the process
}

// working is the state of a worker: how many of those it subscribed to
// are done, and the accumulator of its reduce, if any.
type working struct {
	ended int
	acc   any
}

// WithClock keeps the clock of the process: the window of the end is told
// of when it started.
func (w worker[S, T]) WithClock(now func() time.Time) any {
	w.now = now
	return w
}

func (w worker[S, T]) Init(proc.PID) (working, []actor.Effect, error) {
	var st working
	if w.red != nil {
		st.acc = w.red.init()
	}
	var out []actor.Effect
	for i, from := range w.from {
		sub := stage.Subscribe{To: from, Tag: i, Max: w.max, Partition: w.partition, Ending: stage.Transient, Held: true}
		out = append(out, sub)
	}
	if len(w.from) == 0 {
		st, out = w.done(st, out)
	}
	return st, out, nil
}

func (w worker[S, T]) Dispatcher() stage.Dispatcher { return w.out }

// release has a worker ask for events.
type release struct{}

// HandleInfo releases the subscriptions, once Run has started all the
// workers.
func (w worker[S, T]) HandleInfo(st working, msg any) (working, []actor.Effect) {
	if _, ok := msg.(release); !ok {
		return st, nil
	}
	out := make([]actor.Effect, len(w.from))
	for i := range w.from {
		out[i] = stage.Release{Tag: i}
	}
	return st, out
}

// HandleEvents runs the operations on the events, emitting what they give,
// or folds them into the accumulator.
func (w worker[S, T]) HandleEvents(st working, events []S, _ any) (working, []actor.Effect) {
	if w.red != nil {
		st.acc = w.red.fold(st.acc, events)
		return st, nil
	}
	out := make([]T, 0, len(events))
	push := w.xf(func(t T) { out = append(out, t) })
	for _, e := range events {
		push(e)
	}
	return st, actor.Do(stage.Emit[T]{Events: out})
}

// HandleCancel counts what it subscribed to ended, normally: all of them,
// the worker is done. One failing stops it, Transient, before.
func (w worker[S, T]) HandleCancel(st working, _ any, _ error) (working, []actor.Effect) {
	if st.ended++; st.ended < len(w.from) {
		return st, nil
	}
	return w.done(st, nil)
}

// done lets out what the worker reduced, if it reduces, and is done.
func (w worker[S, T]) done(st working, out []actor.Effect) (working, []actor.Effect) {
	if w.red != nil {
		var start time.Time
		if w.red.every > 0 {
			start = w.clock().Truncate(w.red.every)
		}
		out = append(out, w.flush(st.acc, start))
	}
	return st, append(out, stage.Done{})
}

// Window is the window of a ReduceEvery, none otherwise.
func (w worker[S, T]) Window() time.Duration {
	if w.red == nil {
		return 0
	}
	return w.red.every
}

// HandleWindow lets out the accumulator of the window, and starts afresh.
func (w worker[S, T]) HandleWindow(st working, start time.Time) (working, []actor.Effect) {
	out := w.flush(st.acc, start)
	st.acc = w.red.init()
	return st, actor.Do(out)
}

// flush is the Emit of what the reduce makes of acc.
func (w worker[S, T]) flush(acc any, start time.Time) actor.Effect {
	var events []T
	w.red.flush(acc, start, func(t T) { events = append(events, t) })
	return stage.Emit[T]{Events: events}
}

func (w worker[S, T]) clock() time.Time {
	if w.now == nil {
		return time.Now()
	}
	return w.now()
}

func (worker[S, T]) StopAlone() bool { return true }
