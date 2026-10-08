package main

import (
	"strings"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// lines is a console of the simulation, keeping the lines.
type lines struct{}

func (lines) Init(proc.PID) ([]string, []gen.Effect, error) { return nil, nil, nil }
func (lines) HandleCall(l []string, _ struct{}, _ genserver.From[int]) ([]string, []gen.Effect) {
	return l, nil
}
func (lines) HandleCast(l []string, _ struct{}) ([]string, []gen.Effect) { return l, nil }
func (lines) HandleInfo(l []string, msg any) ([]string, []gen.Effect) {
	return append(l[:len(l):len(l)], msg.(string)), nil
}

// TestFailover crashes the node of the primary: the standby takes over
// from the last count it got.
func TestFailover(t *testing.T) {
	s := gensim.New(1)
	start := func(node string, b gen.Behaviour[work], name string) proc.PID {
		pid, err := gensim.Spawn(s, b, nil, gensim.On(node), gensim.Named(name))
		if err != nil {
			t.Fatal(err)
		}
		return pid
	}
	console, err := gensim.Spawn(s, genserver.Gen(lines{}), nil, gensim.On("a"), gensim.Named(consoleName))
	if err != nil {
		t.Fatal(err)
	}
	every := 100 * time.Millisecond
	start("b", genserver.Gen(Worker{Node: "b", Peer: "a", Primary: true, Every: every}), workerName)
	standby := start("a", genserver.Gen(Worker{Node: "a", Peer: "b", Every: every}), workerName)

	s.Advance(time.Second)
	n, _ := s.Call(standby, struct{}{})
	if n != 10 {
		t.Fatalf("standby got count %v, want 10", n)
	}
	s.Crash("b")
	s.Advance(time.Second)

	out, _ := gensim.State[[]string](s, console)
	text := strings.Join(out, "\n")
	for _, want := range []string{"a: primary gone (noconnection): taking over at count 10", "a: count 11", "a: count 20"} {
		if !strings.Contains(text, want) {
			t.Errorf("no %q in:\n%s", want, text)
		}
	}
}
