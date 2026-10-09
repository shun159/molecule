package gen_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// asyncer starts and cancels asyncs as cast to it, and passes their
// results on to the process to.
type asyncer struct {
	genserver.Default[[]any]
	to proc.PID
}

type (
	startAsync  molecule.Async
	cancelAsync molecule.CancelAsync
)

func (asyncer) HandleCast(s []any, msg any) ([]any, []molecule.Effect) {
	switch m := msg.(type) {
	case startAsync:
		return s, molecule.Do(molecule.Async(m))
	case cancelAsync:
		return s, molecule.Do(molecule.CancelAsync(m))
	case string: // "stop"
		return s, molecule.Do(molecule.Stop{})
	}
	return s, nil
}

func (a asyncer) HandleInfo(s []any, msg any) ([]any, []molecule.Effect) {
	if r, ok := msg.(molecule.AsyncResult); ok {
		s = append(s[:len(s):len(s)], r.Value)
		if !a.to.IsZero() {
			return s, molecule.Do(molecule.Send{To: a.to, Msg: r})
		}
	}
	return s, nil
}

func returns(v any) func(context.Context) (any, error) {
	return func(context.Context) (any, error) { return v, nil }
}

// blocks runs until its ctx is done, which it tells on cancelled.
func blocks(cancelled chan<- struct{}) func(context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		<-ctx.Done()
		close(cancelled)
		return "too late", nil
	}
}

func startAsyncer(t *testing.T, n *proc.Node, to proc.PID) genserver.Ref[genserver.None, genserver.None, any] {
	t.Helper()
	ref, err := genserver.Start(context.Background(), n, asyncer{to: to})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func result(t *testing.T, s *proc.Self) molecule.AsyncResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := s.Receive(ctx)
	r, ok := msg.(molecule.AsyncResult)
	if err != nil || !ok {
		t.Fatalf("got %#v, %v, want an AsyncResult", msg, err)
	}
	return r
}

func noResult(t *testing.T, s *proc.Self) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if msg, err := s.Receive(ctx); err == nil {
		t.Errorf("got %#v", msg)
	}
}

func waitClosed(t *testing.T, c <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: ctx not done", what)
	}
}

// asyncTest runs fn in a process of n.
func asyncTest(t *testing.T, n *proc.Node, fn func(s *proc.Self)) {
	t.Helper()
	done := make(chan struct{})
	n.Spawn(func(s *proc.Self) error {
		defer close(done)
		fn(s)
		return nil
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test process did not finish")
	}
}

func TestAsync(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref := startAsyncer(t, n, s.PID())
		ref.Cast(s, startAsync{Key: "k", Run: returns(42)})
		if r := result(t, s); r.Key != "k" || r.Value != 42 || r.Err != nil {
			t.Errorf("got %+v", r)
		}
	})
}

// An async started under a Key running cancels it: only the new one's
// outcome arrives.
func TestAsyncReplaced(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref := startAsyncer(t, n, s.PID())
		cancelled := make(chan struct{})
		ref.Cast(s, startAsync{Key: "k", Run: blocks(cancelled)})
		ref.Cast(s, startAsync{Key: "k", Run: returns("new")})
		waitClosed(t, cancelled, "replaced")
		if r := result(t, s); r.Value != "new" {
			t.Errorf("got %+v", r)
		}
		noResult(t, s)
	})
}

func TestAsyncCancelled(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref := startAsyncer(t, n, s.PID())
		cancelled := make(chan struct{})
		ref.Cast(s, startAsync{Key: "k", Run: blocks(cancelled)})
		ref.Cast(s, cancelAsync{Key: "k"})
		waitClosed(t, cancelled, "CancelAsync")
		noResult(t, s)
	})
}

// A behaviour terminating cancels its asyncs.
func TestAsyncTerminated(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref := startAsyncer(t, n, s.PID())
		cancelled := make(chan struct{})
		ref.Cast(s, startAsync{Key: "k", Run: blocks(cancelled)})
		ref.Cast(s, "stop")
		waitClosed(t, cancelled, "terminated")
	})
}

// A panic in Run is its error, and ends nothing.
func TestAsyncPanic(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref := startAsyncer(t, n, s.PID())
		ref.Cast(s, startAsync{Key: "k", Run: func(context.Context) (any, error) { panic("boom") }})
		var pe *proc.PanicError
		if r := result(t, s); !errors.As(r.Err, &pe) {
			t.Fatalf("got %+v, want a PanicError", r)
		}
		ref.Cast(s, startAsync{Key: "k", Run: returns("still here")})
		if r := result(t, s); r.Value != "still here" {
			t.Errorf("got %+v", r)
		}
	})
}

// In gensim, an async runs at once, its outcome in flight as any message.
func TestAsyncSimulated(t *testing.T) {
	s := gensim.New(1)
	pid, err := gensim.Spawn(s, genserver.Gen(asyncer{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Cast(pid, startAsync{Key: "a", Run: returns(1)})
	s.Cast(pid, startAsync{Key: "b", Run: returns(2)})
	s.RunUntilIdle()
	got, _ := gensim.State[[]any](s, pid)
	if len(got) != 2 {
		t.Fatalf("results %v, want both", got)
	}
}
