package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Ranges emits the ranges of Chunk numbers from 1 to N-1, as many as
// asked for, and is then done. It holds the demand of its workers until
// it is sent start: so that all have subscribed before it is done, the
// ranges few. Its state is the start of the next range.
type Ranges struct {
	N, Chunk int
}

// start has Ranges emit.
type start struct{}

func (Ranges) Accumulate() bool { return true }

func (Ranges) Init(proc.PID) (int, []actor.Effect, error) { return 1, nil, nil }

func (r Ranges) HandleInfo(next int, msg any) (int, []actor.Effect) {
	if _, ok := msg.(start); ok {
		return next, actor.Do(stage.Forward{})
	}
	return next, nil
}

func (r Ranges) HandleDemand(next, n int) (int, []actor.Effect) {
	var spans []span
	for ; n > 0 && next < r.N; n-- {
		hi := min(next+r.Chunk, r.N)
		spans = append(spans, span{Lo: next, Hi: hi})
		next = hi
	}
	out := actor.Do(stage.Emit[span]{Events: spans})
	if next >= r.N {
		out = append(out, stage.Done{})
	}
	return next, out
}
