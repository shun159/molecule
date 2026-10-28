package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Worker finds the longest chain of each range of its producer. It holds
// two ranges at most: one it works on, and the next, asked for ahead, so
// that it never waits for one to come. A range held by one worker is one
// the others cannot take: the ranges are to be many, for the work to end
// on all the workers at about the same time. Its state is the ranges done.
type Worker struct {
	From actor.Dest
}

func (w Worker) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: w.From, Max: 2}), nil
}

func (Worker) HandleEvents(done int, spans []span, _ any) (int, []actor.Effect) {
	best := make([]chain, len(spans))
	for i, s := range spans {
		best[i] = Longest(s)
	}
	return done + len(spans), actor.Do(stage.Emit[chain]{Events: best})
}
