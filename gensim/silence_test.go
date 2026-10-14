package gensim_test

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// forward sends what it is cast to the process named "log" on node.
type forward struct {
	genserver.Default[struct{}]
	node string
}

func (f forward) HandleCast(s struct{}, msg add) (struct{}, []actor.Effect) {
	return s, actor.Do(actor.Send{To: actor.Remote{Node: f.node, Name: "log"}, Msg: msg})
}

// logOf returns what the nodeLog pid was told, but the nodes up.
func logOf(s *gensim.Sim, pid proc.PID) []any {
	log, _ := gensim.State[[]any](s, pid)
	return slices.DeleteFunc(slices.Clone(log), func(m any) bool {
		_, up := m.(proc.NodeUp)
		return up
	})
}

// A node paused handles nothing, what is sent to it held; the others are
// told it silent after Silent, and a quarter more at most, then heard
// once it resumes, and it handles what was held.
func TestPause(t *testing.T) {
	s := gensim.New(1)
	b := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("b"), gensim.Named("log"))
	a := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("a"))
	f := spawn(t, s, genserver.Gen(forward{node: "b"}), gensim.On("a"))
	s.RunUntilIdle()

	s.Pause("b")
	s.Cast(f, add{1})
	s.Advance(s.Silent - time.Millisecond)
	if log := logOf(s, a); len(log) != 0 {
		t.Fatalf("a told %v before Silent", log)
	}
	if log := logOf(s, b); len(log) != 0 {
		t.Fatalf("b, paused, handled %v", log)
	}
	s.Advance(s.Silent / 4)
	if log := logOf(s, a); !slices.Equal(log, []any{proc.NodeSilent{Node: "b"}}) {
		t.Fatalf("a told %v after Silent", log)
	}

	s.Resume("b")
	s.RunUntilIdle()
	if log := logOf(s, a); !slices.Equal(log, []any{proc.NodeSilent{Node: "b"}, proc.NodeHeard{Node: "b"}}) {
		t.Errorf("a told %v once b resumed", log)
	}
	if log := logOf(s, b); !slices.Equal(log, []any{add{1}}) {
		t.Errorf("b handled %v once resumed", log)
	}
}

// ticking logs what it is told, as nodeLog, and a tick it set a timer for.
type ticking struct{ genserver.Default[[]any] }

type tock struct{}

func (ticking) Init(proc.PID) ([]any, []actor.Effect, error) {
	return nil, actor.Do(actor.StartTimer{Key: tock{}, After: 100 * time.Millisecond, Msg: tock{}}), nil
}

func (ticking) HandleInfo(log []any, msg any) ([]any, []actor.Effect) {
	return append(log[:len(log):len(log)], msg), nil
}

// The timers of a node paused fire into the mailboxes of its processes,
// handled once it resumes, before what was held for them.
func TestPauseTimers(t *testing.T) {
	s := gensim.New(1)
	b := spawn(t, s, genserver.Gen(ticking{}), gensim.On("b"), gensim.Named("log"))
	f := spawn(t, s, genserver.Gen(forward{node: "b"}), gensim.On("a"))
	s.Pause("b")
	s.Cast(f, add{1})
	s.Advance(time.Second)
	if log, _ := gensim.State[[]any](s, b); len(log) != 0 {
		t.Fatalf("b, paused, handled %v", log)
	}
	s.Resume("b")
	s.RunUntilIdle()
	if log, _ := gensim.State[[]any](s, b); !slices.Equal(log, []any{tock{}, add{1}}) {
		t.Errorf("b handled %v", log)
	}
}

// Silenced, nodes hold what they send each other, the connection, links
// and monitors staying; they are told each other silent, and once healed,
// heard, what was held arriving in order.
func TestSilence(t *testing.T) {
	s := gensim.New(1)
	b := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("b"), gensim.Named("log"))
	m := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	l := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	w := spawn(t, s, genserver.Gen(watcher{monitored: m, linked: l, trap: true}), gensim.On("a"))
	a := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("a"))
	f := spawn(t, s, genserver.Gen(forward{node: "b"}), gensim.On("a"))
	s.RunUntilIdle()

	s.Silence([]string{"a"}, []string{"b"})
	s.Cast(f, add{1})
	s.Cast(f, add{2})
	s.Advance(2 * s.Silent)
	if log := logOf(s, b); !slices.Equal(log, []any{proc.NodeSilent{Node: "a"}}) {
		t.Fatalf("b told %v, silenced", log)
	}
	if log := logOf(s, a); !slices.Equal(log, []any{proc.NodeSilent{Node: "b"}}) {
		t.Fatalf("a told %v, silenced", log)
	}
	if log, _ := gensim.State[[]any](s, w); len(log) != 0 || !s.Alive(l) {
		t.Fatalf("links or monitors failed: %v", log)
	}

	s.Heal()
	s.RunUntilIdle()
	want := []any{proc.NodeSilent{Node: "a"}, proc.NodeHeard{Node: "a"}, add{1}, add{2}}
	if log := logOf(s, b); !slices.Equal(log, want) {
		t.Errorf("b told %v, healed; want %v", log, want)
	}
	if log := logOf(s, a); !slices.Equal(log, []any{proc.NodeSilent{Node: "b"}, proc.NodeHeard{Node: "b"}}) {
		t.Errorf("a told %v, healed", log)
	}
}

// Heard again within Silent, a node is never told silent.
func TestSilenceShort(t *testing.T) {
	s := gensim.New(1)
	a := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("a"))
	spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	s.RunUntilIdle()
	s.Silence([]string{"a"}, []string{"b"})
	s.Advance(s.Silent / 2)
	s.Heal()
	s.Pause("b")
	s.Advance(s.Silent / 2)
	s.Resume("b")
	s.Advance(2 * s.Silent)
	if log := logOf(s, a); len(log) != 0 {
		t.Errorf("a told %v", log)
	}
}

// A partition amid a silence loses what was held, and tells the nodes
// down: not heard, but up, once healed.
func TestSilencePartitioned(t *testing.T) {
	s := gensim.New(1)
	b := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("b"), gensim.Named("log"))
	f := spawn(t, s, genserver.Gen(forward{node: "b"}), gensim.On("a"))
	s.RunUntilIdle()
	s.Silence([]string{"a"}, []string{"b"})
	s.Cast(f, add{1})
	s.Advance(2 * s.Silent)
	s.Partition([]string{"a"}, []string{"b"})
	s.Heal()
	s.RunUntilIdle()
	log, _ := gensim.State[[]any](s, b)
	want := []any{proc.NodeSilent{Node: "a"}, proc.NodeDown{Node: "a"}, proc.NodeUp{Node: "a"}}
	if !slices.Equal(log[len(log)-3:], want) {
		t.Errorf("b told %v; want it to end with %v", log, want)
	}
}

// A process monitoring nodes from then on is told those silent, after
// those up.
func TestMonitorNodesSilent(t *testing.T) {
	s := gensim.New(1)
	spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	spawn(t, s, genserver.Gen(counter{}), gensim.On("a"))
	s.Pause("b")
	s.Advance(2 * s.Silent)
	w := spawn(t, s, genserver.Gen(nodeLog{}), gensim.On("a"))
	s.RunUntilIdle()
	log, _ := gensim.State[[]any](s, w)
	want := []any{proc.NodeUp{Node: "b"}, proc.NodeUp{Node: "sim"}, proc.NodeSilent{Node: "b"}}
	if !slices.Equal(log, want) {
		t.Errorf("log %v; want %v", log, want)
	}
}

// A node crashed while paused restarts running.
func TestPauseCrash(t *testing.T) {
	s := gensim.New(1)
	spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	s.Pause("b")
	s.Crash("b")
	s.Restart("b")
	c := spawn(t, s, genserver.Gen(counter{}), gensim.On("b"))
	if v, err := s.Call(c, set{3}); err != nil || v != 3 {
		t.Errorf("call after the restart: %v, %v", v, err)
	}
}
