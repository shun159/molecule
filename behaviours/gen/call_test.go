package gen_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

var errBoom = errors.New("boom")

type echo struct{ n int }

// server is a minimal hand-written server loop. Its state is an int that
// casts add to.
func server(s *proc.Self) error {
	state := 0
	for {
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case actor.CastMsg:
			state += m.Req.(int)
		case actor.CallMsg:
			switch req := m.Req.(type) {
			case echo:
				actor.SendReply(s, m.From, req)
			case string:
				switch req {
				case "get":
					actor.SendReply(s, m.From, state)
				case "from":
					actor.SendReply(s, m.From, m.From)
				case "crash":
					return errBoom
				case "reply-then-crash":
					actor.SendReply(s, m.From, "bye")
					return errBoom
				case "ignore":
				case "stop":
					actor.SendReply(s, m.From, "ok")
					return nil
				}
			}
		}
	}
}

// startServer starts a server and stops it when the test ends.
func startServer(t *testing.T, n *proc.Node) proc.PID {
	t.Helper()
	pid := n.Spawn(server)
	t.Cleanup(func() { actor.Call(context.Background(), n, pid, "stop") })
	return pid
}

func call(t *testing.T, caller actor.Caller, to actor.Dest, req any) any {
	t.Helper()
	v, err := actor.Call(context.Background(), caller, to, req)
	if err != nil {
		t.Fatalf("Call(%v, %v): %v", to, req, err)
	}
	return v
}

func TestCallAndCast(t *testing.T) {
	n := proc.NewNode("")
	srv := startServer(t, n)

	if v := call(t, n, srv, "get"); v != 0 {
		t.Errorf("get = %v", v)
	}
	actor.SendCast(n, srv, 5)
	actor.SendCast(n, srv, 2)
	if v := call(t, n, srv, "get"); v != 7 {
		t.Errorf("get after casts = %v", v)
	}
}

func TestCallFrom(t *testing.T) {
	n := proc.NewNode("")
	srv := startServer(t, n)

	if from := call(t, n, srv, "from").(actor.From); !from.PID.IsZero() || from.Tag.IsZero() {
		t.Errorf("From of a node caller = %+v", from)
	}

	type result struct {
		self proc.PID
		from actor.From
	}
	got := make(chan result, 1)
	n.Spawn(func(s *proc.Self) error {
		v, err := actor.Call(context.Background(), s, srv, "from")
		if err != nil {
			return err
		}
		got <- result{s.PID(), v.(actor.From)}
		return nil
	})
	if r := <-got; r.from.PID != r.self {
		t.Errorf("From.PID = %v, want the calling process %v", r.from.PID, r.self)
	}
}

func TestCallLocalName(t *testing.T) {
	n := proc.NewNode("")
	srv := startServer(t, n)
	if err := actor.Local("srv").Register(n, srv); err != nil {
		t.Fatal(err)
	}
	if pid, ok := actor.Local("srv").WhereIs(n); !ok || pid != srv {
		t.Errorf("WhereIs = %v, %v", pid, ok)
	}
	actor.SendCast(n, actor.Local("srv"), 3)
	if v := call(t, n, actor.Local("srv"), "get"); v != 3 {
		t.Errorf("get = %v", v)
	}

	_, err := actor.Call(context.Background(), n, actor.Local("nobody"), "get")
	var ee *actor.ExitError
	if !errors.As(err, &ee) || !errors.Is(err, proc.NoProc) || ee.To != actor.Local("nobody") {
		t.Errorf("call to unregistered name: %v", err)
	}
	actor.SendCast(n, actor.Local("nobody"), 1) // dropped
}

func TestCallNoServer(t *testing.T) {
	n := proc.NewNode("a@host")
	dead := n.Spawn(func(*proc.Self) error { return nil })
	ctx, stop := n.Watch(context.Background(), dead)
	<-ctx.Done()
	stop()
	remote := proc.NewNode("b@host").Spawn(func(*proc.Self) error { return nil })

	for _, tt := range []struct {
		to   actor.Dest
		want error
	}{
		{dead, proc.NoProc},
		{proc.PID{}, proc.NoProc},
		{remote, proc.NoConnection},
	} {
		if _, err := actor.Call(context.Background(), n, tt.to, "get"); !errors.Is(err, tt.want) {
			t.Errorf("Call(%v) = %v, want %v", tt.to, err, tt.want)
		}
	}
}

func TestCallServerCrash(t *testing.T) {
	n := proc.NewNode("")
	srv := n.Spawn(server)
	_, err := actor.Call(context.Background(), n, srv, "crash")
	var ee *actor.ExitError
	if !errors.As(err, &ee) || ee.Reason != errBoom || ee.To != srv {
		t.Errorf("Call = %v, want an exit with %v", err, errBoom)
	}
}

func TestCallReplyThenCrash(t *testing.T) {
	n := proc.NewNode("")
	for range 200 {
		srv := n.Spawn(server)
		if v, err := actor.Call(context.Background(), n, srv, "reply-then-crash"); v != "bye" || err != nil {
			t.Fatalf("Call = %v, %v; want the reply sent before the crash", v, err)
		}
	}
}

func TestCallTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := proc.NewNode("")
		srv := n.Spawn(server)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		start := time.Now()
		_, err := actor.Call(ctx, n, srv, "ignore")
		if err != context.DeadlineExceeded || time.Since(start) != time.Second {
			t.Errorf("Call = %v after %v", err, time.Since(start))
		}
		if v := call(t, n, srv, "get"); v != 0 {
			t.Errorf("server unusable after a timed out call: %v", v)
		}
		call(t, n, srv, "stop")
	})
}

func TestCallSelf(t *testing.T) {
	n := proc.NewNode("")
	errc := make(chan error, 1)
	n.Spawn(func(s *proc.Self) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := actor.Call(ctx, s, s.PID(), "get")
		errc <- err
		return nil
	})
	if err := <-errc; err != actor.ErrCallingSelf {
		t.Errorf("Call = %v", err)
	}
}

func TestCallerKilled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := proc.NewNode("")
		srv := n.Spawn(server)
		errc := make(chan error, 1)
		caller := n.Spawn(func(s *proc.Self) error {
			_, err := actor.Call(context.Background(), s, srv, "ignore")
			errc <- err
			return err
		})
		synctest.Wait() // the call is in flight and ignored

		n.Spawn(func(s *proc.Self) error { s.Exit(caller, proc.Kill); return nil })
		if err := <-errc; err != proc.Killed {
			t.Errorf("Call = %v, want killed", err)
		}
		call(t, n, srv, "stop")
	})
}

func TestConcurrentCalls(t *testing.T) {
	n := proc.NewNode("")
	srv := startServer(t, n)
	var wg sync.WaitGroup
	for c := range 50 {
		wg.Go(func() {
			for i := range 100 {
				want := echo{c*1000 + i}
				if v, err := actor.Call(context.Background(), n, srv, want); v != want || err != nil {
					t.Errorf("got %v, %v; want %v", v, err, want)
					return
				}
			}
		})
	}
	wg.Wait()
}
