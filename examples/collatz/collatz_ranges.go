package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Ranges emits the ranges of Chunk numbers from 1 to N-1, as many as
// asked for, and is then done. It holds the demand until it is sent
// start, as GenStage's demand: :accumulate: so that all its workers have
// subscribed before it is done, the ranges few.
type Ranges struct {
	N, Chunk int
}

// start has Ranges emit.
type start struct{}

type ranges struct {
	next    int // the start of the next range
	asked   int // the ranges asked for and not emitted
	started bool
}

func (Ranges) Init(proc.PID) (ranges, []actor.Effect, error) { return ranges{next: 1}, nil, nil }

func (r Ranges) HandleDemand(s ranges, n int) (ranges, []actor.Effect) {
	s.asked += n
	return r.emit(s)
}

func (r Ranges) HandleInfo(s ranges, msg any) (ranges, []actor.Effect) {
	if _, ok := msg.(start); !ok {
		return s, nil
	}
	s.started = true
	return r.emit(s)
}

// emit emits the ranges asked for, once started, and is done after the
// last.
func (r Ranges) emit(s ranges) (ranges, []actor.Effect) {
	if !s.started {
		return s, nil
	}
	var spans []span
	for ; s.asked > 0 && s.next < r.N; s.asked-- {
		hi := min(s.next+r.Chunk, r.N)
		spans = append(spans, span{Lo: s.next, Hi: hi})
		s.next = hi
	}
	out := actor.Do(stage.Emit[span]{Events: spans})
	if s.next >= r.N {
		out = append(out, stage.Done{})
	}
	return s, out
}
