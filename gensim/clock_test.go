package gensim_test

import (
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// stamp is a server answering with the time, of the clock it is given.
type stamp struct {
	genserver.Default[struct{}]
	now func() time.Time
}

func (s stamp) WithClock(now func() time.Time) any { s.now = now; return s }

func (s stamp) HandleCall(st struct{}, _ get, from genserver.From[time.Time]) (struct{}, []molecule.Effect) {
	return st, molecule.Do(from.Reply(s.now()))
}

// stampMachine is stamp as a state machine.
type stampMachine struct{ now func() time.Time }

func (m stampMachine) WithClock(now func() time.Time) any { m.now = now; return m }

func (stampMachine) Init(proc.PID) (int, struct{}, []molecule.Effect, error) {
	return 0, struct{}{}, nil, nil
}

func (m stampMachine) HandleEvent(st int, d struct{}, ev genstatem.Event) (int, struct{}, []molecule.Effect) {
	if c, ok := ev.(genstatem.Call); ok {
		return st, d, molecule.Do(c.Reply(m.now()))
	}
	return st, d, nil
}

// A Clocked behaviour reads the clock of its process: in a simulation, the
// virtual one, moving only as the simulation's time passes.
func TestClocked(t *testing.T) {
	s := gensim.New(1)
	server := spawn(t, s, genserver.Gen(stamp{}))
	machine := spawn(t, s, genstatem.Gen(stampMachine{}))
	for _, pid := range []proc.PID{server, machine} {
		v, err := s.Call(pid, get{})
		if err != nil || !v.(time.Time).Equal(s.Now()) {
			t.Fatalf("%v: %v, %v, want the simulation's %v", pid, v, err, s.Now())
		}
	}
	s.Advance(time.Hour)
	for _, pid := range []proc.PID{server, machine} {
		if v, _ := s.Call(pid, get{}); !v.(time.Time).Equal(s.Now()) {
			t.Errorf("%v after an hour: %v, want %v", pid, v, s.Now())
		}
	}
}

// wrongClock returns another behaviour from WithClock.
type wrongClock struct{ genserver.Default[int] }

func (wrongClock) WithClock(func() time.Time) any { return stamp{} }

// A WithClock returning another type of behaviour fails the start.
func TestClockedWrongType(t *testing.T) {
	s := gensim.New(1)
	if _, err := gensim.Spawn(s, genserver.Gen(wrongClock{}), nil); err == nil {
		t.Error("started")
	}
}
