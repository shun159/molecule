package gensim_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// relay casts what it is cast to the counter named "counter" on node.
type relay struct{ node string }

func (relay) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (relay) HandleCall(s struct{}, _ get, _ genserver.From[int]) (struct{}, []molecule.Effect) {
	return s, nil
}

func (r relay) HandleCast(s struct{}, msg add) (struct{}, []molecule.Effect) {
	return s, molecule.Do(molecule.Cast{To: molecule.Remote{Node: r.node, Name: "counter"}, Req: msg})
}

func TestNodes(t *testing.T) {
	s := gensim.New(1)
	c := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
	r := spawn(t, s, genserver.Gen(relay{"b"}), gensim.On("a"))
	if c.Node() != "b" || r.Node() != "a" {
		t.Fatalf("nodes %s, %s", c.Node(), r.Node())
	}
	s.Cast(r, add{2})
	s.RunUntilIdle()
	if v, err := s.Call(molecule.Remote{Node: "b", Name: "counter"}, get{}); err != nil || v != 2 {
		t.Errorf("counter of b: %v, %v", v, err)
	}
	// A Local name is of the default node, where there is none.
	if _, ok := s.WhereIs("counter"); ok {
		t.Error("counter found on the default node")
	}
}

func TestPartition(t *testing.T) {
	for _, trap := range []bool{false, true} {
		s := gensim.New(1)
		m := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
		l := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
		w := spawn(t, s, genserver.Gen(watcher{monitored: m, linked: l, trap: trap}), gensim.On("a"))
		r := spawn(t, s, genserver.Gen(relay{"b"}), gensim.On("a"))
		s.RunUntilIdle()

		s.Cast(r, add{1})
		s.Step() // the cast reaches the relay...
		s.Step() // ...which sends it on, in flight when the network splits
		s.Partition([]string{"a"}, []string{"b"})
		s.RunUntilIdle()
		if v, _ := s.Call(m, get{}); v != 0 {
			t.Errorf("trap %v: counter %v, the cast in flight arrived", trap, v)
		}
		if !trap {
			if s.Alive(w) {
				t.Error("watcher alive, its link lost")
			}
		} else {
			log, _ := gensim.State[[]any](s, w)
			want := []any{
				proc.ExitMsg{From: l, Reason: proc.NoConnection},
				molecule.Down{Tag: "m", PID: m, Reason: proc.NoConnection},
			}
			if !sameItems(log, want) {
				t.Errorf("watcher log %#v", log)
			}
		}
		// The other side lost its link too.
		if s.Alive(l) {
			t.Errorf("trap %v: linked process of b alive", trap)
		}

		// Partitioned, casts are lost; healed, they arrive.
		s.Cast(r, add{1})
		s.RunUntilIdle()
		s.Heal()
		s.Cast(r, add{10})
		s.RunUntilIdle()
		if v, _ := s.Call(m, get{}); v != 10 {
			t.Errorf("trap %v: counter %v after healing", trap, v)
		}
	}
}

func sameItems(got, want []any) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || g == w
		}
		if !found {
			return false
		}
	}
	return true
}

func TestCrashRestart(t *testing.T) {
	s := gensim.New(1)
	old := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
	l := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	w := spawn(t, s, genserver.Gen(watcher{monitored: old, linked: l, trap: true}), gensim.On("a"))
	s.Cast(old, add{5})
	s.RunUntilIdle()

	s.Crash("b")
	s.RunUntilIdle()
	if s.Alive(old) {
		t.Error("process of a crashed node alive")
	}
	log, _ := gensim.State[[]any](s, w)
	if !sameItems(log, []any{
		proc.ExitMsg{From: l, Reason: proc.NoConnection},
		molecule.Down{Tag: "m", PID: old, Reason: proc.NoConnection},
	}) {
		t.Errorf("watcher log %#v", log)
	}
	if _, err := s.Call(molecule.Remote{Node: "b", Name: "counter"}, get{}); !errors.Is(err, proc.NoProc) {
		t.Errorf("call to a crashed node: %v", err)
	}
	if _, err := s.Call(old, get{}); !errors.Is(err, proc.NoConnection) {
		t.Errorf("call to a process of a crashed node: %v", err)
	}
	if _, err := gensim.Spawn(s, genserver.Gen(counter{}), nil, gensim.On("b")); !errors.Is(err, gensim.ErrNodeDown) {
		t.Errorf("spawn on a crashed node: %v", err)
	}

	s.Restart("b")
	c := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
	if c == old || s.Alive(old) {
		t.Error("previous incarnation back")
	}
	if v, err := s.Call(molecule.Remote{Node: "b", Name: "counter"}, get{}); err != nil || v != 0 {
		t.Errorf("counter after restart: %v, %v", v, err)
	}
}

func TestLoss(t *testing.T) {
	s := gensim.New(1)
	s.Loss = 1
	spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
	r := spawn(t, s, genserver.Gen(relay{"b"}), gensim.On("a"))
	local := spawn(t, s, genserver.Gen(relay{"a"}), gensim.On("a"))
	spawn(t, s, genserver.Gen(counter{}), gensim.On("a"), gensim.Named("counter"))
	s.Cast(r, add{1})     // lost between a and b
	s.Cast(local, add{1}) // within a, kept
	s.RunUntilIdle()
	for node, want := range map[string]int{"a": 1, "b": 0} {
		if v, _ := s.Call(molecule.Remote{Node: node, Name: "counter"}, get{}); v != want {
			t.Errorf("counter of %s: %v, want %d", node, v, want)
		}
	}
}

// TestFaultsDeterministic runs the same faults with the same seed twice:
// the traces are the same.
func TestFaultsDeterministic(t *testing.T) {
	run := func() string {
		s := gensim.New(7)
		s.Loss = 0.3
		spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
		r := spawn(t, s, genserver.Gen(relay{"b"}), gensim.On("a"))
		for i := range 20 {
			s.Cast(r, add{i})
			s.Step()
			if i == 10 {
				s.Partition([]string{"a"}, []string{"b"})
			}
			if i == 15 {
				s.Heal()
			}
		}
		s.RunUntilIdle()
		return s.TraceString()
	}
	if a, b := run(), run(); a != b {
		t.Error("same seed, different runs")
	}
}

// TestPendingRequest has a request to another node waiting when the nodes
// lose each other: it fails with NoConnection, for a partition as for a
// crash.
func TestPendingRequest(t *testing.T) {
	for _, fault := range []string{"partition", "crash"} {
		s := gensim.New(1)
		c := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
		r := spawn(t, s, genserver.Gen(requester{c}), gensim.On("a"))
		s.Cast(r, goNow{})
		s.RunUntilIdle() // the request is at c, which never answers
		if fault == "partition" {
			s.Partition([]string{"a"}, []string{"b"})
		} else {
			s.Crash("b")
		}
		s.RunUntilIdle()
		log, _ := gensim.State[[]any](s, r)
		if len(log) != 2 || !errors.Is(log[1].(molecule.Response).Err, proc.NoConnection) {
			t.Errorf("%s: %#v", fault, log)
		}
	}
}

func TestRun(t *testing.T) {
	s := gensim.New(1)
	c := spawn(t, s, genserver.Gen(counter{}))
	for range 3 {
		s.Cast(c, add{1})
	}
	steps := 0
	if err := s.Run(time.Second, func() error { steps++; return nil }); err != nil || steps != 6 {
		t.Errorf("Run: %v after %d steps, want 6", err, steps)
	}
	if got := s.Now().Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)); got != time.Second {
		t.Errorf("time %v", got)
	}
	// The first error stops it, at the step that broke the invariant.
	for range 3 {
		s.Cast(c, add{1})
	}
	tooMuch := errors.New("too much")
	err := s.Run(time.Second, func() error {
		if n, _ := gensim.State[int](s, c); n > 4 {
			return tooMuch
		}
		return nil
	})
	if n, _ := gensim.State[int](s, c); err != tooMuch || n != 5 {
		t.Errorf("Run: %v at %d", err, n)
	}
}

// remoteWatcher monitors and requests the counter of b by name.
type remoteWatcher struct{ genserver.Default[[]any] }

func (remoteWatcher) HandleCast(log []any, _ goNow) ([]any, []molecule.Effect) {
	counter := molecule.Remote{Node: "b", Name: "counter"}
	return log, molecule.Do(
		molecule.SendRequest{To: counter, Req: get{}, Tag: "get"},
		molecule.Monitor{Target: counter, Tag: "counter"},
	)
}

func (remoteWatcher) HandleInfo(log []any, msg any) ([]any, []molecule.Effect) {
	return append(log[:len(log):len(log)], msg), nil
}

func TestRemoteNameEffects(t *testing.T) {
	s := gensim.New(1)
	c := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"), gensim.Named("counter"))
	s.Cast(c, add{3})
	w := spawn(t, s, genserver.Gen(remoteWatcher{}), gensim.On("a"))
	s.Cast(w, goNow{})
	s.RunUntilIdle()
	log, _ := gensim.State[[]any](s, w)
	if len(log) != 1 || log[0] != (molecule.Response{Tag: "get", Value: 3}) {
		t.Fatalf("log %#v", log)
	}
	s.Partition([]string{"a"}, []string{"b"})
	s.RunUntilIdle()
	log, _ = gensim.State[[]any](s, w)
	if len(log) != 2 || log[1] != (molecule.Down{Tag: "counter", PID: c, Reason: proc.NoConnection}) {
		t.Errorf("log %#v", log)
	}
	// Cut apart, both fail at once.
	s.Cast(w, goNow{})
	s.RunUntilIdle()
	log, _ = gensim.State[[]any](s, w)
	if len(log) != 4 {
		t.Fatalf("log %#v", log)
	}
	for _, m := range log[2:] {
		switch m := m.(type) {
		case molecule.Response:
			if !errors.Is(m.Err, proc.NoConnection) {
				t.Errorf("response %#v", m)
			}
		case molecule.Down:
			if m.Reason != proc.NoConnection {
				t.Errorf("down %#v", m)
			}
		}
	}
}

// nodeLog monitors nodes, and keeps what it is told.
type nodeLog struct{ genserver.Default[[]any] }

func (nodeLog) Init(proc.PID) ([]any, []molecule.Effect, error) {
	return nil, molecule.Do(molecule.MonitorNodes{On: true}), nil
}

func (nodeLog) HandleInfo(log []any, msg any) ([]any, []molecule.Effect) {
	return append(log[:len(log):len(log)], msg), nil
}

func TestMonitorNodes(t *testing.T) {
	s := gensim.New(1)
	spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	w := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("a"))
	spawn(t, s, genserver.Gen(counter{}), gensim.On("c"))
	s.Partition([]string{"a"}, []string{"b"})
	s.Heal()
	s.Crash("c")
	s.Restart("c")
	s.RunUntilIdle()
	log, _ := gensim.State[[]any](s, w)
	want := []any{
		proc.NodeUp{Node: "b"}, proc.NodeUp{Node: "sim"}, // at once
		proc.NodeUp{Node: "c"}, // made after
		proc.NodeDown{Node: "b"}, proc.NodeUp{Node: "b"},
		proc.NodeDown{Node: "c"}, proc.NodeUp{Node: "c"},
	}
	if !slices.Equal(log, want) {
		t.Errorf("log %#v\nwant %#v", log, want)
	}
}
