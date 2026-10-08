package genstatem_test

import (
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// scripted returns, for each cast step, the state and actions it carries,
// and logs every other event, Enter included.
type scripted struct{}

type step struct {
	to   int
	effs []molecule.Effect
}

func (scripted) StateEnter() bool { return true }

func (scripted) Init(proc.PID) (int, []any, []molecule.Effect, error) { return 0, nil, nil, nil }

func (scripted) HandleEvent(st int, log []any, ev genstatem.Event) (int, []any, []molecule.Effect) {
	if c, ok := ev.(genstatem.Cast); ok {
		if s, ok := c.Msg.(step); ok {
			return s.to, log, s.effs
		}
	}
	log = append(slices.Clip(log), ev)
	if c, ok := ev.(genstatem.Cast); ok && c.Msg == "postpone" {
		return st, log, molecule.Do(genstatem.Postpone{})
	}
	return st, log, nil
}

func start(t *testing.T) (*gensim.Sim, proc.PID, func() []any) {
	t.Helper()
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genstatem.Gen[int, []any](scripted{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.RunUntilIdle()
	return s, pid, func() []any {
		m, _ := gensim.State[genstatem.Machine[int, []any]](s, pid)
		return m.Data()[1:] // after the initial Enter
	}
}

func do(s *gensim.Sim, pid proc.PID, to int, effs ...molecule.Effect) {
	s.Cast(pid, step{to, effs})
	s.RunUntilIdle()
}

func TestUpdateStateTimeout(t *testing.T) {
	s, pid, log := start(t)
	do(s, pid, 0, genstatem.StartStateTimeout{After: 2 * time.Second, Msg: "a"})
	s.Advance(time.Second)
	do(s, pid, 0, genstatem.UpdateStateTimeout{Msg: "b"})
	s.Advance(900 * time.Millisecond)
	if got := log(); len(got) != 0 {
		t.Fatalf("restarted? %#v", got)
	}
	s.Advance(200 * time.Millisecond)
	if got := log(); !slices.Equal(got, []any{genstatem.StateTimeout{Msg: "b"}}) {
		t.Errorf("log %#v", got)
	}
	// None running: it fires at once.
	do(s, pid, 0, genstatem.UpdateStateTimeout{Msg: "c"})
	if got := log(); len(got) != 2 || got[1] != (genstatem.StateTimeout{Msg: "c"}) {
		t.Errorf("log %#v", got)
	}
}

func TestAbsoluteTimeout(t *testing.T) {
	s, pid, log := start(t)
	do(s, pid, 0, genstatem.StartStateTimeout{At: s.Now().Add(3 * time.Second), Msg: "at"})
	s.Advance(2900 * time.Millisecond)
	if got := log(); len(got) != 0 {
		t.Fatalf("early: %#v", got)
	}
	s.Advance(200 * time.Millisecond)
	if got := log(); !slices.Equal(got, []any{genstatem.StateTimeout{Msg: "at"}}) {
		t.Errorf("log %#v", got)
	}
	// In the past: at once.
	do(s, pid, 0, genstatem.StartTimeout{Name: "g", At: s.Now().Add(-time.Hour), Msg: "past"})
	s.Advance(0)
	if got := log(); len(got) != 2 || got[1] != (genstatem.Timeout{Name: "g", Msg: "past"}) {
		t.Errorf("log %#v", got)
	}
}

func TestGenericAndEventUpdates(t *testing.T) {
	s, pid, log := start(t)
	do(s, pid, 0,
		genstatem.StartTimeout{Name: "g", After: time.Second, Msg: "x"},
		genstatem.UpdateTimeout{Name: "g", Msg: "y"},
		genstatem.UpdateTimeout{Name: "h", Msg: "now"},
		genstatem.StartTimeout{Name: "k", After: time.Second, Msg: "cancelled"},
		genstatem.CancelTimeout{Name: "k"},
	)
	if got := log(); !slices.Equal(got, []any{genstatem.Timeout{Name: "h", Msg: "now"}}) {
		t.Fatalf("log %#v", got)
	}
	s.Advance(2 * time.Second)
	if got := log(); len(got) != 2 || got[1] != (genstatem.Timeout{Name: "g", Msg: "y"}) {
		t.Errorf("log %#v", got)
	}
	do(s, pid, 0,
		genstatem.StartEventTimeout{After: time.Second, Msg: "e"},
		genstatem.UpdateEventTimeout{Msg: "f"},
	)
	if got := log(); len(got) != 2 {
		t.Fatalf("event timeout fired at once: %#v", got)
	}
	s.Advance(2 * time.Second)
	if got := log(); len(got) != 3 || got[2] != (genstatem.EventTimeout{Msg: "f"}) {
		t.Errorf("log %#v", got)
	}
}

func TestRepeatState(t *testing.T) {
	s, pid, log := start(t)
	s.Cast(pid, "postpone")
	s.RunUntilIdle()
	do(s, pid, 0, genstatem.RepeatState{})
	// The enter call again, the postponed event not retried.
	want := []any{genstatem.Cast{Msg: "postpone"}, genstatem.Enter[int]{Old: 0}}
	if got := log(); !slices.Equal(got, want) {
		t.Fatalf("log %#v", got)
	}
	do(s, pid, 1)
	want = append(want, genstatem.Enter[int]{Old: 0}, genstatem.Cast{Msg: "postpone"})
	if got := log(); !slices.Equal(got, want) {
		t.Errorf("after a change: %#v", got)
	}
}
