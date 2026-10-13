package gen_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
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
	startAsync  actor.Async
	cancelAsync actor.CancelAsync
)

func (asyncer) HandleCast(s []any, msg any) ([]any, []actor.Effect) {
	switch m := msg.(type) {
	case startAsync:
		return s, actor.Do(actor.Async(m))
	case cancelAsync:
		return s, actor.Do(actor.CancelAsync(m))
	case string: // "stop"
		return s, actor.Do(actor.Stop{})
	case []actor.Effect:
		return s, m
	}
	return s, nil
}

func (a asyncer) HandleInfo(s []any, msg any) ([]any, []actor.Effect) {
	if r, ok := msg.(actor.AsyncResult); ok {
		s = append(s[:len(s):len(s)], r.Value)
		if !a.to.IsZero() {
			return s, actor.Do(actor.Send{To: a.to, Msg: r})
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

func result(t *testing.T, s *proc.Self) actor.AsyncResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := s.Receive(ctx)
	r, ok := msg.(actor.AsyncResult)
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

// gated tells on started it runs, and returns i once released.
func gated(i int, started chan<- int, release <-chan struct{}) func(context.Context) (any, error) {
	return func(ctx context.Context) (any, error) {
		started <- i
		select {
		case <-release:
		case <-ctx.Done():
		}
		return i, nil
	}
}

// WithMaxAsync runs as many asyncs at once, the others waiting their
// turn, in order; one cancelled while waiting never runs.
func TestMaxAsync(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref, err := genserver.Start(context.Background(), n, asyncer{to: s.PID()}, actor.WithMaxAsync(2))
		if err != nil {
			t.Fatal(err)
		}
		started := make(chan int, 4)
		var release [4]chan struct{}
		for i := range release {
			release[i] = make(chan struct{})
			ref.Cast(s, startAsync{Key: i, Run: gated(i, started, release[i])})
		}
		ref.Cast(s, cancelAsync{Key: 3})
		running := map[int]bool{<-started: true, <-started: true}
		if !running[0] || !running[1] {
			t.Fatalf("running %v, want 0 and 1", running)
		}
		for i := range 3 {
			close(release[i])
			if r := result(t, s); r.Value != i {
				t.Fatalf("got %+v, want %d", r, i)
			}
			if i == 0 {
				if j := <-started; j != 2 {
					t.Fatalf("%d started, want 2", j)
				}
			}
		}
		noResult(t, s)
		select {
		case j := <-started:
			t.Errorf("%d started", j)
		default:
		}
	})
}

// A running async cancelled keeps its place until its Run returns, even
// one not heeding its ctx: no more than WithMaxAsync goroutines run.
func TestMaxAsyncCancelled(t *testing.T) {
	n := proc.NewNode("")
	asyncTest(t, n, func(s *proc.Self) {
		ref, err := genserver.Start(context.Background(), n, asyncer{to: s.PID()}, actor.WithMaxAsync(1))
		if err != nil {
			t.Fatal(err)
		}
		started := make(chan int, 2)
		release := make(chan struct{})
		stubborn := func(context.Context) (any, error) {
			started <- 0
			<-release
			return 0, nil
		}
		ref.Cast(s, startAsync{Key: 0, Run: stubborn})
		<-started
		ref.Cast(s, cancelAsync{Key: 0})
		ref.Cast(s, startAsync{Key: 1, Run: gated(1, started, nil)})
		select {
		case <-started:
			t.Fatal("1 started while the cancelled 0 runs")
		case <-time.After(50 * time.Millisecond):
		}
		close(release)
		if j := <-started; j != 1 {
			t.Fatalf("%d started, want 1", j)
		}
		ref.Cast(s, cancelAsync{Key: 1})
		noResult(t, s)
	})
}

// In gensim as well: asyncs waiting run in order, one at a time here, a
// cancelled one never, a replaced one at the end.
func TestMaxAsyncSimulated(t *testing.T) {
	for seed := range uint64(5) {
		s := gensim.New(seed)
		pid, err := gensim.Start(s, genserver.Child(asyncer{}, actor.WithMaxAsync(1)))
		if err != nil {
			t.Fatal(err)
		}
		ran := map[int]bool{}
		run := func(v any) func(context.Context) (any, error) {
			return func(context.Context) (any, error) {
				if i, ok := v.(int); ok {
					ran[i] = true
				}
				return v, nil
			}
		}
		var effs []actor.Effect
		for i := range 5 {
			effs = append(effs, actor.Async{Key: i, Run: run(i)})
		}
		effs = append(effs, actor.CancelAsync{Key: 2}, actor.Async{Key: 3, Run: run("3b")})
		s.Cast(pid, effs)
		s.RunUntilIdle()
		got, _ := gensim.State[[]any](s, pid)
		if want := []any{0, 1, 4, "3b"}; !slices.Equal(got, want) {
			t.Errorf("seed %d: results %v, want %v", seed, got, want)
		}
		if ran[2] || ran[3] {
			t.Errorf("seed %d: ran %v", seed, ran)
		}
	}
}
