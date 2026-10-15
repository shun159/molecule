package main

import (
	"slices"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/dist/pg"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// recorder keeps what it is sent, in place of the console's printer.
type recorder struct{ genserver.Default[[]string] }

func (recorder) HandleInfo(got []string, msg any) ([]string, []actor.Effect) {
	if p, ok := msg.(Published); ok {
		return append(slices.Clip(got), p.String()), nil
	}
	return got, nil
}

// What a node publishes reaches the subscribers of every node, once; a
// node cut off misses it, and does not get it once back.
func TestPublish(t *testing.T) {
	s := gensim.New(1)
	brokers := map[string]proc.PID{}
	for _, node := range []string{"a", "b", "c"} {
		gensim.Spawn(s, genserver.Gen(pg.Scope{Name: string(scope)}), nil, gensim.On(node), gensim.Named(string(scope)))
		brokers[node], _ = gensim.Spawn(s, genserver.Gen(Broker{Node: node}), nil, gensim.On(node), gensim.Named(string(brokerName)))
	}
	out := map[string]proc.PID{}
	for _, node := range []string{"b", "c"} {
		out[node], _ = gensim.Spawn(s, genserver.Gen(recorder{}), nil, gensim.On(node))
		gensim.Spawn(s, genserver.Gen(Subscriber{Topic: "news", Out: out[node]}), nil, gensim.On(node))
	}
	s.RunUntilIdle()
	got := func(node string) []string {
		g, _ := gensim.State[[]string](s, out[node])
		return g
	}

	s.Cast(brokers["a"], publish{"news", "hello"})
	s.Cast(brokers["a"], publish{"other", "to none"})
	s.RunUntilIdle()
	for _, node := range []string{"b", "c"} {
		if g := got(node); !slices.Equal(g, []string{"[news] hello (from a)"}) {
			t.Errorf("%s got %q", node, g)
		}
	}

	s.Partition([]string{"a", "b"}, []string{"c"})
	s.RunUntilIdle()
	s.Cast(brokers["a"], publish{"news", "while c is away"})
	s.RunUntilIdle()
	s.Heal()
	s.RunUntilIdle()
	if g := got("b"); len(g) != 2 {
		t.Errorf("b got %q", g)
	}
	if g := got("c"); len(g) != 1 {
		t.Errorf("c, cut off, got %q; want what was published before only", g)
	}
}
