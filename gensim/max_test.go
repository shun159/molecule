package gensim_test

import (
	"testing"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// A server flooded past WithMaxMessages is killed with proc.MailboxFull;
// without it, it takes everything.
func TestMaxMessages(t *testing.T) {
	for seed := range uint64(5) {
		s := gensim.New(seed)
		limited, err := gensim.Start(s, genserver.Child(counter{}, molecule.WithMaxMessages(2)))
		if err != nil {
			t.Fatal(err)
		}
		free, err := gensim.Start(s, genserver.Child(counter{}))
		if err != nil {
			t.Fatal(err)
		}
		for range 50 {
			s.Cast(limited, add{1})
			s.Cast(free, add{1})
		}
		s.RunUntilIdle()
		if s.Alive(limited) {
			t.Fatalf("seed %d: flooded past its limit, alive", seed)
		}
		var reason any
		for _, e := range s.Trace() {
			if e.Kind == gensim.Exited && e.To == limited {
				reason = e.Msg
			}
		}
		if reason != proc.MailboxFull {
			t.Errorf("seed %d: exited with %v", seed, reason)
		}
		if n, err := s.Call(free, get{}); n != 50 || err != nil {
			t.Errorf("seed %d: unlimited got %v %v", seed, n, err)
		}
	}
}
