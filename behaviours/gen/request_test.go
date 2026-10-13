package gen_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// holder is a server holding each call until told to reply to it.
func holder(t *testing.T, n *proc.Node) (proc.PID, <-chan actor.From) {
	t.Helper()
	calls := make(chan actor.From, 4)
	pid := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if c, ok := msg.(actor.CallMsg); ok {
				calls <- c.From
			}
		}
	})
	return pid, calls
}

func pending[Rep any](t *testing.T, p *actor.Pending[Rep]) {
	t.Helper()
	select {
	case <-p.Done():
		t.Fatal("done before the reply")
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := p.Result(); err != actor.ErrNotDone {
		t.Errorf("Result before the reply: %v", err)
	}
}

func TestRequest(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	pid, calls := holder(t, n)

	p := actor.Request[int](n, pid, "q")
	from := <-calls
	pending(t, p)
	actor.SendReply(n, from, 7)
	<-p.Done()
	if v, err := p.Result(); v != 7 || err != nil {
		t.Errorf("Result: %v, %v", v, err)
	}

	// Waiting gives up with its context; the request stays.
	p = actor.Request[int](n, pid, "q")
	from = <-calls
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := p.Wait(short); err != context.DeadlineExceeded {
		t.Errorf("Wait: %v", err)
	}
	actor.SendReply(n, from, 8)
	if v, err := p.Wait(ctx); v != 8 || err != nil {
		t.Errorf("Wait after: %v, %v", v, err)
	}

	// Cancelled, the reply is dropped.
	p = actor.Request[int](n, pid, "q")
	from = <-calls
	p.Cancel()
	actor.SendReply(n, from, 9)
	if _, err := p.Result(); err != actor.ErrCancelled {
		t.Errorf("cancelled: %v", err)
	}

	// A reply of another type.
	ps := actor.Request[string](n, pid, "q")
	actor.SendReply(n, <-calls, 10)
	if _, err := ps.Wait(ctx); err == nil {
		t.Error("reply of the wrong type taken")
	}

	// The server dies first.
	p = actor.Request[int](n, pid, "q")
	<-calls
	boom := errors.New("boom")
	n.Spawn(func(s *proc.Self) error { s.Exit(pid, boom); return nil })
	var exit *actor.ExitError
	if _, err := p.Wait(ctx); !errors.As(err, &exit) || exit.Reason != boom {
		t.Errorf("server died: %v", err)
	}

	// No server: done at once.
	p = actor.Request[int](n, actor.Local("nobody"), "q")
	<-p.Done()
	if _, err := p.Result(); !errors.Is(err, proc.NoProc) {
		t.Errorf("no server: %v", err)
	}
}

// TestRequestCallerDies has the caller die while its request is pending.
func TestRequestCallerDies(t *testing.T) {
	n := proc.NewNode("")
	pid, calls := holder(t, n)
	got := make(chan *actor.Pending[int], 1)
	caller := n.Spawn(func(s *proc.Self) error {
		got <- actor.Request[int](s, pid, "q")
		_, err := s.Receive(context.Background())
		return err
	})
	p := <-got
	<-calls
	n.Spawn(func(s *proc.Self) error { s.Exit(caller, proc.Kill); return nil })
	if _, err := p.Wait(context.Background()); err != proc.Killed {
		t.Errorf("caller killed: %v", err)
	}
}
