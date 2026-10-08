package main

import (
	"fmt"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// Worker counts, a tick at a time. The primary sends each count to the
// standby on the other node; the standby monitors the primary, and when
// it is gone, its node crashed or cut off, takes over from the last count
// it got.
type Worker struct {
	genserver.Default[work] // it takes no call

	Node    string // its own node
	Peer    string // the node of the other worker
	Primary bool
	Every   time.Duration
}

// work is the state of a worker.
type work struct {
	self    proc.PID
	active  bool     // counting, rather than standing by
	count   int      // the last count, made or received
	primary proc.PID // the primary watched, by a standby
}

// checkpoint is a count, from the primary to the standby.
type checkpoint struct {
	Count int
	From  proc.PID
}

type tick struct{}

const workerName = "worker"

func (w Worker) Init(self proc.PID) (work, []gen.Effect, error) {
	s := work{self: self, active: w.Primary}
	if !w.Primary {
		return s, gen.Do(w.say("standing by")), nil
	}
	return s, gen.Do(w.next()), nil
}

// HandleCast takes a checkpoint of the primary, and watches it.
func (w Worker) HandleCast(s work, c checkpoint) (work, []gen.Effect) {
	if s.active {
		return s, nil
	}
	s.count = c.Count
	if c.From == s.primary {
		return s, nil
	}
	s.primary = c.From
	return s, gen.Do(
		w.say(fmt.Sprintf("watching the primary %v", c.From)),
		gen.Monitor{Target: c.From, Tag: "primary"},
	)
}

func (w Worker) HandleInfo(s work, msg any) (work, []gen.Effect) {
	switch m := msg.(type) {
	case tick:
		s.count++
		return s, gen.Do(
			w.say(fmt.Sprintf("count %d", s.count)),
			gen.Cast{To: gen.Remote{Node: w.Peer, Name: workerName}, Req: checkpoint{s.count, s.self}},
			w.next(),
		)
	case gen.Down:
		s.active = true
		return s, gen.Do(
			w.say(fmt.Sprintf("primary gone (%v): taking over at count %d", m.Reason, s.count)),
			w.next(),
		)
	}
	return s, nil
}

func (w Worker) next() gen.Effect { return gen.StartTimer{Key: "tick", After: w.Every, Msg: tick{}} }

// say prints line on the console of the node.
func (w Worker) say(line string) gen.Effect {
	return gen.Send{To: gen.Local(consoleName), Msg: w.Node + ": " + line}
}
