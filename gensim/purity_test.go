package gensim_test

import (
	"maps"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gensim"
)

// tally counts casts by name in a map: in place, or on a copy.
type tally struct {
	genserver.Default[map[string]int]
	copy bool
}

func (t tally) HandleCast(m map[string]int, name string) (map[string]int, []molecule.Effect) {
	if t.copy {
		m = maps.Clone(m)
	} else if m == nil {
		m = map[string]int{}
	}
	m[name]++
	return m, nil
}

// ranks keeps a slice, its first element overwritten in place.
type ranks struct{ genserver.Default[[]string] }

func (ranks) HandleCast(r []string, name string) ([]string, []molecule.Effect) {
	if len(r) == 0 {
		return []string{name}, nil
	}
	r[0] = name
	return r, nil
}

// impure runs fn and returns what it panicked with, if an ImpureError.
func impure(fn func()) (err *gensim.ImpureError) {
	defer func() {
		if r := recover(); r != nil {
			e, ok := r.(*gensim.ImpureError)
			if !ok {
				panic(r)
			}
			err = e
		}
	}()
	fn()
	return nil
}

func TestPurity(t *testing.T) {
	for _, tt := range []struct {
		name   string
		b      func(s *gensim.Sim) error
		impure bool
	}{
		{"map in place", func(s *gensim.Sim) error { _, err := gensim.Spawn(s, genserver.Gen(tally{}), nil); return err }, true},
		{"map copied", func(s *gensim.Sim) error {
			_, err := gensim.Spawn(s, genserver.Gen(tally{copy: true}), nil)
			return err
		}, false},
		{"slice in place", func(s *gensim.Sim) error { _, err := gensim.Spawn(s, genserver.Gen(ranks{}), nil); return err }, true},
	} {
		s := gensim.New(1)
		if err := tt.b(s); err != nil {
			t.Fatal(err)
		}
		pid := s.Trace()[0].To
		err := impure(func() {
			s.Cast(pid, "a")
			s.Cast(pid, "b")
			s.RunUntilIdle()
		})
		if (err != nil) != tt.impure {
			t.Errorf("%s: %v", tt.name, err)
		}
		if err != nil && (err.PID != pid || err.Msg != (molecule.CastMsg{Req: "b"})) {
			t.Errorf("%s: %+v", tt.name, err)
		}
	}
}

func TestPurityOfMessages(t *testing.T) {
	s := gensim.New(1)
	pid := spawn(t, s, genserver.Gen(recorder{}))
	msg := []string{"sent"}
	s.Send(pid, msg)
	msg[0] = "changed"
	// The driver sent it: the error tells no process.
	if err := impure(s.RunUntilIdle); err == nil || !err.PID.IsZero() {
		t.Errorf("message changed in flight: %v", err)
	}

	// Without the check, it goes.
	s = gensim.New(1, gensim.WithoutPurityCheck())
	pid = spawn(t, s, genserver.Gen(tally{}))
	if err := impure(func() {
		s.Cast(pid, "a")
		s.Cast(pid, "a")
		s.RunUntilIdle()
	}); err != nil {
		t.Errorf("checked: %v", err)
	}
}

// clock keeps a time, which it formats: no change of its own.
type clock struct{ genserver.Default[time.Time] }

func (clock) HandleCast(t time.Time, _ string) (time.Time, []molecule.Effect) {
	_ = t.Local().String()
	return t.Add(time.Second), nil
}

func TestPurityOfTimes(t *testing.T) {
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genserver.Gen(clock{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := impure(func() {
		s.Cast(pid, "tick")
		s.Cast(pid, "tick")
		s.RunUntilIdle()
	}); err != nil {
		t.Error(err)
	}
}
