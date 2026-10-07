package main

import (
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

// EchoStats is a gen_server counting connections and echoed bytes. The
// protocol processes cast events to it; anyone can call it for the totals.
type EchoStats struct{}

// Stats is both the state of EchoStats and its reply to GetStats.
type Stats struct {
	Open  int   // connections now open
	Total int   // connections ever opened
	Bytes int64 // bytes echoed
}

// GetStats is the only call.
type GetStats struct{}

// StatsEvent is what the protocol processes cast.
type StatsEvent interface{ statsEvent() }

type (
	connOpened struct{}
	connClosed struct{}
	echoed     struct{ n int }
)

func (connOpened) statsEvent() {}
func (connClosed) statsEvent() {}
func (echoed) statsEvent()     {}

// statsName is where EchoStats is registered, and statsRef how to reach it.
var (
	statsName = gen.Local("echo_stats")
	statsRef  = genserver.RefFor(EchoStats{}, statsName)
)

func statsChildSpec(id string) supervisor.ChildSpec {
	return supervisor.ChildSpec{
		ID:    id,
		Start: genserver.StartLinkFunc(EchoStats{}, gen.WithName(statsName)),
	}
}

func (EchoStats) Init(proc.PID) (Stats, []gen.Effect, error) { return Stats{}, nil, nil }

func (EchoStats) HandleCall(s Stats, _ GetStats, from genserver.From[Stats]) (Stats, []gen.Effect) {
	return s, gen.Do(from.Reply(s))
}

func (EchoStats) HandleCast(s Stats, ev StatsEvent) (Stats, []gen.Effect) {
	switch e := ev.(type) {
	case connOpened:
		s.Open++
		s.Total++
	case connClosed:
		s.Open--
	case echoed:
		s.Bytes += int64(e.n)
	}
	return s, nil
}
