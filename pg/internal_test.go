package pg

import (
	"reflect"
	"testing"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

// TestCallbacksArePure checks that no callback modifies the state it got.
func TestCallbacksArePure(t *testing.T) {
	n := proc.NewNode("")
	a, b := n.NewPID(), n.NewPID()
	s, _, _ := Scope{}.Init(proc.PID{})
	s, _ = Scope{}.HandleCast(s, join{"g", []proc.PID{a, b}})
	before := deepCopy(s) // not clone, which is under test

	Scope{}.HandleCast(s, join{"g", []proc.PID{a}})
	Scope{}.HandleCast(s, join{"h", []proc.PID{a}})
	Scope{}.HandleCast(s, leave{"g", []proc.PID{a, b}})
	Scope{}.HandleInfo(s, molecule.Down{Tag: a, PID: a})
	if !reflect.DeepEqual(s, before) {
		t.Errorf("state modified:\n%+v\nwas\n%+v", s, before)
	}
}

func deepCopy(s state) state {
	c := state{groups: map[any][]proc.PID{}, joins: map[proc.PID]int{}}
	c.order = append([]any(nil), s.order...)
	for g, m := range s.groups {
		c.groups[g] = append([]proc.PID(nil), m...)
	}
	for p, n := range s.joins {
		c.joins[p] = n
	}
	return c
}
