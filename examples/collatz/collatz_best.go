package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Best keeps the longest chain of its workers, and tells Done once they
// are all done.
type Best struct {
	From []actor.Dest
	Done actor.From
}

type best struct {
	chain chain
	ended int // the workers done
}

// Init subscribes to each worker, Transient: a worker done leaves Best to
// wait for the others.
func (b Best) Init(proc.PID) (best, []actor.Effect, error) {
	var out []actor.Effect
	for i, w := range b.From {
		out = append(out, stage.Subscribe{To: w, Tag: i, Max: 100, Ending: stage.Transient})
	}
	return best{chain: chain{Steps: -1}}, out, nil
}

func (Best) HandleEvents(b best, chains []chain, _ any) (best, []actor.Effect) {
	for _, c := range chains {
		if c.Steps > b.chain.Steps || c.Steps == b.chain.Steps && c.Start < b.chain.Start {
			b.chain = c
		}
	}
	return b, nil
}

func (x Best) HandleCancel(b best, _ any, _ error) (best, []actor.Effect) {
	b.ended++
	if b.ended < len(x.From) {
		return b, nil
	}
	return b, actor.Do(actor.Reply{To: x.Done, Value: b.chain}, actor.Stop{})
}
