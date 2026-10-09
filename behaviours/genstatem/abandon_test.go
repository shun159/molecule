package genstatem_test

import (
	"context"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// worker answers one call at a time, after work: a call arriving while it
// works is postponed. It counts the calls it took up, and the abandoned
// ones it was told of.
type worker struct{}

type workerData struct {
	from      molecule.From // the call worked for
	taken     int
	abandoned int
}

type (
	work   struct{}
	done   struct{}
	counts struct{}
)

const (
	idle = iota
	busy
)

func (worker) Init(proc.PID) (int, workerData, []molecule.Effect, error) {
	return idle, workerData{}, nil, nil
}

func (worker) HandleEvent(st int, d workerData, ev genstatem.Event) (int, workerData, []molecule.Effect) {
	switch e := ev.(type) {
	case genstatem.Cast:
		if _, ok := e.Msg.(counts); ok {
			return st, d, nil
		}
	case genstatem.Call:
		if _, ok := e.Req.(counts); ok {
			return st, d, molecule.Do(e.Reply([2]int{d.taken, d.abandoned}))
		}
		if st == busy {
			return st, d, molecule.Do(genstatem.Postpone{})
		}
		d.from = e.From
		d.taken++
		return busy, d, molecule.Do(genstatem.StartStateTimeout{After: time.Second, Msg: done{}})
	case genstatem.StateTimeout:
		return idle, d, molecule.Do(molecule.Reply{To: d.from, Value: "done"})
	case genstatem.Info:
		if ab, ok := e.Msg.(molecule.CallAbandoned); ok && ab.From == d.from {
			d.abandoned++
			return idle, d, nil
		}
	}
	return st, d, nil
}

// A call abandoned while worked for is told as Info, in gensim too.
func TestAbandoned(t *testing.T) {
	s := gensim.New(1)
	s.CallTimeout = 100 * time.Millisecond
	pid, err := gensim.Spawn(s, genstatem.Gen[int, workerData](worker{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	// Taken up, then given up on before the work is done.
	if _, err := s.Call(pid, work{}); err != context.DeadlineExceeded {
		t.Fatalf("first Call = %v", err)
	}
	s.RunUntilIdle()
	s.CallTimeout = time.Minute
	got, err := s.Call(pid, counts{})
	if err != nil {
		t.Fatal(err)
	}
	if got != [2]int{1, 1} {
		t.Fatalf("taken, abandoned = %v, want [1 1]", got)
	}
}

// A call made while the worker works waits, postponed; its caller giving
// up drops it, never to be taken up.
func TestAbandonedPostponed(t *testing.T) {
	n := proc.NewNode("")
	pid, err := genstatem.Start(context.Background(), n, worker{})
	if err != nil {
		t.Fatal(err)
	}
	ref := genstatem.NewRef(pid)
	first := ref.SendRequest(n, work{}) // taken up, for a second
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := ref.Call(ctx, n, work{}); err != context.DeadlineExceeded {
		t.Fatalf("second Call = %v", err)
	}
	if v, err := first.Wait(context.Background()); err != nil || v != "done" {
		t.Fatalf("first = %v, %v", v, err)
	}
	got, err := ref.Call(context.Background(), n, counts{})
	if err != nil {
		t.Fatal(err)
	}
	if got != [2]int{1, 0} {
		t.Fatalf("taken, abandoned = %v, want [1 0]: the postponed call ran", got)
	}
}
