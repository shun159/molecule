package gensim_test

import (
	"testing"

	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
)

// run casts to two counters at once, and returns the trace of the run.
func run(t *testing.T, opts ...gensim.Option) string {
	t.Helper()
	s := gensim.New(7, opts...)
	a := spawn(t, s, genserver.Gen(counter{}))
	b := spawn(t, s, genserver.Gen(counter{}))
	for i := range 5 {
		s.Cast(a, add{i})
		s.Cast(b, add{i})
	}
	s.RunUntilIdle()
	return s.TraceString()
}

// The choices given decide the order of a run, the same choices the same
// order; none given, or once used up, the seed decides.
func TestWithChoices(t *testing.T) {
	if run(t) != run(t, gensim.WithChoices(nil)) {
		t.Error("no choices, another order than the seed's")
	}
	zeros, ones := make([]byte, 64), make([]byte, 64)
	for i := range ones {
		ones[i] = 1
	}
	if first, again := run(t, gensim.WithChoices(zeros)), run(t, gensim.WithChoices(zeros)); first != again {
		t.Error("same choices, different orders")
	}
	if run(t, gensim.WithChoices(zeros)) == run(t, gensim.WithChoices(ones)) {
		t.Error("other choices, the same order")
	}
	short := []byte{1, 0, 1}
	if first, again := run(t, gensim.WithChoices(short)), run(t, gensim.WithChoices(short)); first != again {
		t.Error("choices used up, different orders")
	}
}
