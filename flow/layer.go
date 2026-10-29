package flow

import (
	"context"

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
		return stage.Start(ctx, n, stage.ProduceConsume(worker[S, T]{wiring: w, out: out, xf: f.xf}))
	}}
}

// worker runs the operations of a layer on the events it subscribes to.
// It subscribes to all, Transient and held, until Run, having started all
// the workers, releases them: no worker comes late to another, or to a
// source, done before it subscribed. It is done once all it subscribed
// to are; its consumer gone, it stops. Its state is how many are done.
type worker[S, T any] struct {
	wiring
	out stage.Dispatcher
	xf  func(emit func(T)) func(S)
}

func (w worker[S, T]) Init(proc.PID) (int, []actor.Effect, error) {
	var out []actor.Effect
	for i, from := range w.from {
		sub := stage.Subscribe{To: from, Tag: i, Max: w.max, Partition: w.partition, Ending: stage.Transient, Held: true}
		out = append(out, sub)
	}
	if len(w.from) == 0 {
		out = append(out, stage.Done{})
	}
	return 0, out, nil
}

func (w worker[S, T]) Dispatcher() stage.Dispatcher { return w.out }

// release has a worker ask for events.
type release struct{}

// HandleInfo releases the subscriptions, once Run has started all the
// workers.
func (w worker[S, T]) HandleInfo(ended int, msg any) (int, []actor.Effect) {
	if _, ok := msg.(release); !ok {
		return ended, nil
	}
	out := make([]actor.Effect, len(w.from))
	for i := range w.from {
		out[i] = stage.Release{Tag: i}
	}
	return ended, out
}

// HandleEvents runs the operations on the events, emitting what they give.
func (w worker[S, T]) HandleEvents(ended int, events []S, _ any) (int, []actor.Effect) {
	out := make([]T, 0, len(events))
	push := w.xf(func(t T) { out = append(out, t) })
	for _, e := range events {
		push(e)
	}
	return ended, actor.Do(stage.Emit[T]{Events: out})
}

// HandleCancel counts what it subscribed to ended, normally: all of them,
// the worker is done. One failing stops it, Transient, before.
func (w worker[S, T]) HandleCancel(ended int, _ any, _ error) (int, []actor.Effect) {
	if ended++; ended < len(w.from) {
		return ended, nil
	}
	return ended, actor.Do(stage.Done{})
}

func (worker[S, T]) StopAlone() bool { return true }
