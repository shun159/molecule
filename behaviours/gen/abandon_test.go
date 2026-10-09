package gen_test

import (
	"context"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// recorder is a server never replying, passing on to its parent what it
// gets.
func recorder(parent proc.PID) func(*proc.Self) error {
	return func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			s.Send(parent, msg)
		}
	}
}

// abandoned waits for the CallMsg the recorder got, then for the
// CallAbandoned of the same From.
func abandoned(t *testing.T, s *proc.Self) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := s.Receive(ctx)
	call, ok := msg.(molecule.CallMsg)
	if err != nil || !ok {
		t.Fatalf("got %#v, %v, want the CallMsg", msg, err)
	}
	msg, err = s.Receive(ctx)
	ab, ok := msg.(molecule.CallAbandoned)
	if err != nil || !ok || ab.From != call.From {
		t.Fatalf("got %#v, %v, want the CallAbandoned of %v", msg, err, call.From)
	}
}

func inProcess(t *testing.T, n *proc.Node, fn func(s *proc.Self)) {
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

func TestCallAbandoned(t *testing.T) {
	n := proc.NewNode("")
	inProcess(t, n, func(s *proc.Self) {
		srv := s.Spawn(recorder(s.PID()))
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := molecule.Call(ctx, s, srv, "never answered"); err != context.DeadlineExceeded {
			t.Fatalf("Call = %v", err)
		}
		abandoned(t, s)
	})
}

// A call answered is not abandoned.
func TestCallAnsweredNotAbandoned(t *testing.T) {
	n := proc.NewNode("")
	inProcess(t, n, func(s *proc.Self) {
		srv := n.Spawn(server)
		if _, err := molecule.Call(context.Background(), s, srv, echo{1}); err != nil {
			t.Fatal(err)
		}
		molecule.Call(context.Background(), s, srv, "stop")
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if msg, err := s.Receive(ctx); err == nil {
			t.Errorf("got %#v", msg)
		}
	})
}

func TestRequestCancelAbandoned(t *testing.T) {
	n := proc.NewNode("")
	inProcess(t, n, func(s *proc.Self) {
		srv := s.Spawn(recorder(s.PID()))
		p := molecule.Request[any](s, srv, "never answered")
		time.Sleep(10 * time.Millisecond)
		p.Cancel()
		abandoned(t, s)
	})
}

// requester sends a request with a timeout to the PID cast to it.
type requester struct{}

func (requester) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (requester) HandleCall(s struct{}, _ struct{}, _ genserver.From[struct{}]) (struct{}, []molecule.Effect) {
	return s, nil
}

func (requester) HandleCast(s struct{}, to proc.PID) (struct{}, []molecule.Effect) {
	return s, molecule.Do(molecule.SendRequest{To: to, Req: "never answered", Tag: "t", Timeout: 20 * time.Millisecond})
}

func TestSendRequestTimeoutAbandoned(t *testing.T) {
	n := proc.NewNode("")
	inProcess(t, n, func(s *proc.Self) {
		srv := s.Spawn(recorder(s.PID()))
		ref, err := genserver.Start(context.Background(), n, requester{})
		if err != nil {
			t.Fatal(err)
		}
		ref.Cast(s, srv)
		abandoned(t, s)
	})
}
