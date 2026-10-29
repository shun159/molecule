package flow

import (
	"context"
	"iter"
	"runtime"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Option is an option of Run.
type Option func(*options)

type options struct {
	workers int
	max     int
}

// Workers runs n workers: as many as the cores, if not given.
func Workers(n int) Option { return func(o *options) { o.workers = n } }

// Max is the Max of every subscription of the flow: each worker asks for
// as many events ahead of its sources, the range for as many of each
// worker. stage.DefaultMax if not given.
func Max(n int) Option { return func(o *options) { o.max = n } }

// Run runs f, its workers on n, and returns what it gives to range over:
// each event, and, if a worker or a source ends other than normally, its
// reason once, at the end. Breaking off the range stops the workers.
func Run[S, T any](ctx context.Context, n *proc.Node, f Flow[S, T], opts ...Option) iter.Seq2[T, error] {
	o := options{workers: runtime.GOMAXPROCS(0), max: stage.DefaultMax}
	for _, opt := range opts {
		opt(&o)
	}
	return func(yield func(T, error) bool) {
		var zero T
		subs, err := start(ctx, n, f, o)
		if err != nil {
			yield(zero, err)
			return
		}
		// The range subscribed to the workers, then the workers to their
		// sources: none is gone, nor any source, before all have.
		p, err := stage.StartPull[T](ctx, n, subs...)
		if err != nil {
			terminate(n, subs)
			yield(zero, err)
			return
		}
		for _, s := range subs {
			n.Send(s.To.(proc.PID), release{})
		}
		for v, err := range p.All() {
			if !yield(v, err) {
				return
			}
		}
	}
}

// start starts the workers of f, and returns the subscriptions to them;
// those started, if one fails, it terminates.
func start[S, T any](ctx context.Context, n *proc.Node, f Flow[S, T], o options) ([]stage.Subscribe, error) {
	var subs []stage.Subscribe
	for range o.workers {
		w, err := stage.Start(ctx, n, stage.ProduceConsume(worker[S, T]{from: f.from, xf: f.xf, max: o.max}))
		if err != nil {
			terminate(n, subs)
			return nil, err
		}
		subs = append(subs, stage.Subscribe{To: w, Max: o.max})
	}
	return subs, nil
}

// terminate terminates the workers of subs: held, with no consumer, they
// would wait for ever.
func terminate(n *proc.Node, subs []stage.Subscribe) {
	for _, s := range subs {
		gen.Terminate(context.Background(), n, s.To, proc.Shutdown)
	}
}

// worker runs the operations of a flow on the events of its sources. It
// subscribes to all, Transient and held, until Run, having started all
// the workers, releases them: no worker comes late to a source done
// before it subscribed. It is done once its sources are; its consumer
// gone, it stops. Its state is the sources ended.
type worker[S, T any] struct {
	from []actor.Dest
	xf   func(emit func(T)) func(S)
	max  int
}

func (w worker[S, T]) Init(proc.PID) (int, []actor.Effect, error) {
	var out []actor.Effect
	for i, from := range w.from {
		out = append(out, stage.Subscribe{To: from, Tag: i, Max: w.max, Ending: stage.Transient, Held: true})
	}
	if len(w.from) == 0 {
		out = append(out, stage.Done{})
	}
	return 0, out, nil
}

// release has a worker ask its sources for events.
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

// HandleCancel counts the sources ended, normally: all of them, the worker
// is done. A source failing stops it, Transient, before.
func (w worker[S, T]) HandleCancel(ended int, _ any, _ error) (int, []actor.Effect) {
	if ended++; ended < len(w.from) {
		return ended, nil
	}
	return ended, actor.Do(stage.Done{})
}

func (worker[S, T]) StopAlone() bool { return true }
