package gensim_test

import (
	"strings"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// A simulation tells the events of its processes as a node does.
func TestEvents(t *testing.T) {
	var evs []any
	s := gensim.New(1, gensim.WithEvents(func(e any) { evs = append(evs, e) }))
	c := spawn(t, s, genserver.Gen(counter{}), gensim.Named("counter"))
	s.Call(c, crash{})
	limited, err := gensim.Start(s, genserver.Child(counter{}, actor.WithMaxMessages(1)))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		s.Cast(limited, add{1})
	}
	s.RunUntilIdle()
	stopped := spawn(t, s, genserver.Gen(counter{}))
	s.Exit(stopped, proc.Shutdown)
	s.RunUntilIdle()

	if len(evs) != 2 {
		t.Fatalf("events %v, want 2", evs)
	}
	cr, ok := evs[0].(proc.Crashed)
	if !ok || cr.PID != c || cr.Name != "counter" || !strings.Contains(cr.Label, "counter") || cr.Reason == nil {
		t.Errorf("first %#v, want counter crashed", evs[0])
	}
	if o, ok := evs[1].(proc.Overflowed); !ok || o.PID != limited || o.Len != 2 {
		t.Errorf("second %#v, want limited overflowed", evs[1])
	}
}
