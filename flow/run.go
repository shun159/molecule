package flow

import (
	"cmp"
	"context"
	"iter"
	"runtime"
	"slices"

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
	layers := append(slices.Clip(f.done), layerOf(f, nil))
	return func(yield func(T, error) bool) {
		var zero T
		started, err := start(ctx, n, f.from, layers, o)
		if err != nil {
			yield(zero, err)
			return
		}
		// The range subscribed to the last workers, and every worker to
		// those before, or the sources: none is gone, nor any source,
		// before all have.
		var subs []stage.Subscribe
		for _, w := range started[len(started)-1] {
			subs = append(subs, stage.Subscribe{To: w, Max: o.max})
		}
		p, err := stage.StartPull[T](ctx, n, subs...)
		if err != nil {
			terminate(n, started)
			yield(zero, err)
			return
		}
		for _, ws := range started {
			for _, w := range ws {
				n.Send(w, release{})
			}
		}
		for v, err := range p.All() {
			if !yield(v, err) {
				return
			}
		}
	}
}

// start starts the workers of the layers, each subscribed to those of the
// layer before, the first to the sources, and returns them, by layer;
// those started, if one fails, it terminates.
func start(ctx context.Context, n *proc.Node, sources []actor.Dest, layers []layer, o options) ([][]proc.PID, error) {
	var started [][]proc.PID
	from := sources
	for i, l := range layers {
		count := cmp.Or(l.stages, o.workers)
		ws := make([]proc.PID, 0, count)
		for k := range count {
			w := wiring{from: from, max: o.max}
			if i > 0 && layers[i-1].partitioned {
				w.partition = k
			}
			pid, err := l.start(ctx, n, w)
			if err != nil {
				terminate(n, append(started, ws))
				return nil, err
			}
			ws = append(ws, pid)
		}
		started = append(started, ws)
		from = dests(ws)
	}
	return started, nil
}

func dests(pids []proc.PID) []actor.Dest {
	ds := make([]actor.Dest, len(pids))
	for i, pid := range pids {
		ds[i] = pid
	}
	return ds
}

// terminate terminates the workers: held, with no consumer, they would
// wait for ever.
func terminate(n *proc.Node, started [][]proc.PID) {
	for _, ws := range started {
		for _, w := range ws {
			gen.Terminate(context.Background(), n, w, proc.Shutdown)
		}
	}
}
