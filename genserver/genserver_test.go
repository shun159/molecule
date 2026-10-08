package genserver_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

var errBoom = errors.New("boom")

func TestCounterPure(t *testing.T) {
	var from genserver.From[int]
	n, effs := Counter{}.HandleCall(3, Reset{}, from)
	if n != 0 || !reflect.DeepEqual(effs, gen.Do(gen.Reply{To: from.From, Value: 3})) {
		t.Errorf("HandleCall(3, Reset) = %d, %#v", n, effs)
	}
	if n, effs := (Counter{}).HandleCast(3, Add{4}); n != 7 || effs != nil {
		t.Errorf("HandleCast(3, Add{4}) = %d, %#v", n, effs)
	}
}

// echo implements all the optional callbacks: infos it logs in its state,
// Terminate it reports to the observer.
type echo struct{ observer proc.PID }

type echoState struct {
	infos    []any
	observer proc.PID
}

type stopped struct{ reason error }

func (e echo) Init(proc.PID) (echoState, []gen.Effect, error) {
	return echoState{observer: e.observer}, nil, nil
}

func (echo) HandleCall(s echoState, req string, from genserver.From[string]) (echoState, []gen.Effect) {
	if req == "stop" {
		return s, gen.Do(from.Reply("bye"), gen.Stop{Reason: errBoom})
	}
	return s, gen.Do(from.Reply(req))
}

func (echo) HandleCast(s echoState, _ struct{}) (echoState, []gen.Effect) { return s, nil }

func (echo) HandleInfo(s echoState, msg any) (echoState, []gen.Effect) {
	s.infos = append(s.infos[:len(s.infos):len(s.infos)], msg)
	return s, nil
}

func (echo) Terminate(s echoState, reason error) []gen.Effect {
	return gen.Do(gen.Send{To: s.observer, Msg: stopped{reason}})
}

func collector(n *proc.Node) (proc.PID, <-chan any) {
	ch := make(chan any, 16)
	pid := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			ch <- msg
		}
	})
	return pid, ch
}

func kill(n *proc.Node, pid proc.PID) {
	ctx, stop := n.Watch(context.Background(), pid)
	defer stop()
	n.Spawn(func(s *proc.Self) error { s.Exit(pid, proc.Kill); return nil })
	<-ctx.Done()
}

func TestOptionalCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := proc.NewNode("")
		ctx := context.Background()
		observer, events := collector(n)
		defer kill(n, observer)

		e, err := genserver.Start(ctx, n, echo{observer})
		if err != nil {
			t.Fatal(err)
		}
		n.Send(e.Dest().(proc.PID), "info")
		if v, err := e.Call(ctx, n, "hi"); v != "hi" || err != nil {
			t.Errorf("Call = %q, %v", v, err)
		}
		s, _ := gen.GetState(ctx, n, e.Dest())
		if got := s.(echoState).infos; !reflect.DeepEqual(got, []any{"info"}) {
			t.Errorf("infos = %v", got)
		}
		if v, _ := e.Call(ctx, n, "stop"); v != "bye" {
			t.Errorf("stop = %q", v)
		}
		if ev := <-events; ev != (stopped{errBoom}) {
			t.Errorf("Terminate reported %#v", ev)
		}

		// Counter has neither: infos are dropped and it keeps running.
		c, _ := genserver.Start(ctx, n, Counter{})
		defer kill(n, c.Dest().(proc.PID))
		n.Send(c.Dest().(proc.PID), "info")
		if v, err := c.Call(ctx, n, Get{}); v != 0 || err != nil {
			t.Errorf("Call after an info = %d, %v", v, err)
		}
	})
}

func TestBadMessage(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	for _, send := range []func(gen.Dest){
		func(d gen.Dest) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			gen.Call(ctx, n, d, "not a CounterReq")
		},
		func(d gen.Dest) { gen.SendCast(n, d, "not an Add") },
	} {
		c, _ := genserver.Start(ctx, n, Counter{})
		down, stop := n.Watch(ctx, c.Dest().(proc.PID))
		send(c.Dest())
		select {
		case <-down.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("server kept running after a bad message")
		}
		stop()
		var bad *genserver.BadMessageError
		if !errors.As(context.Cause(down), &bad) || bad.Msg == nil {
			t.Errorf("exit reason = %v", context.Cause(down))
		}
	}
}

func TestWrongReplyType(t *testing.T) {
	n := proc.NewNode("")
	pid := n.Spawn(func(s *proc.Self) error {
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		gen.SendReply(s, msg.(gen.CallMsg).From, "not an int")
		return nil
	})
	_, err := genserver.NewRef[CounterReq, int, Add](pid).Call(context.Background(), n, Get{})
	if err == nil {
		t.Error("Call accepted a reply of the wrong type")
	}
}

func TestCallErrors(t *testing.T) {
	n := proc.NewNode("")
	ref := genserver.RefFor(Counter{}, gen.Local("nobody"))
	if v, err := ref.Call(context.Background(), n, Get{}); v != 0 || !errors.Is(err, proc.NoProc) {
		t.Errorf("Call = %d, %v", v, err)
	}
	ref.Cast(n, Add{1}) // dropped
}

// relay forwards Get to a counter without waiting, through CallEffect, and
// replies to its own caller when the response arrives.
type relay struct {
	counter genserver.Ref[CounterReq, int, Add]
}

type relayState struct {
	pending map[int]genserver.From[int]
	next    int
}

func (relay) Init(proc.PID) (relayState, []gen.Effect, error) {
	return relayState{pending: map[int]genserver.From[int]{}}, nil, nil
}

func (r relay) HandleCall(s relayState, _ Get, from genserver.From[int]) (relayState, []gen.Effect) {
	pending := maps.Clone(s.pending)
	pending[s.next] = from
	return relayState{pending: pending, next: s.next + 1}, gen.Do(r.counter.CallEffect(Get{}, s.next))
}

func (relay) HandleCast(s relayState, _ struct{}) (relayState, []gen.Effect) { return s, nil }

func (relay) HandleInfo(s relayState, msg any) (relayState, []gen.Effect) {
	resp, ok := msg.(gen.Response)
	if !ok {
		return s, nil
	}
	from := s.pending[resp.Tag.(int)]
	pending := maps.Clone(s.pending)
	delete(pending, resp.Tag.(int))
	return relayState{pending: pending, next: s.next}, gen.Do(from.Reply(resp.Value.(int)))
}

func TestCallEffect(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	counter, _ := genserver.Start(ctx, n, Counter{Initial: 42})
	r, _ := genserver.Start(ctx, n, relay{counter})
	if v, err := r.Call(ctx, n, Get{}); v != 42 || err != nil {
		t.Errorf("relayed Get = %d, %v", v, err)
	}
	counter.Cast(n, Add{1})
	if v, err := r.Call(ctx, n, Get{}); v != 43 || err != nil {
		t.Errorf("relayed Get = %d, %v", v, err)
	}
}

func TestStartLink(t *testing.T) {
	n := proc.NewNode("")
	got := make(chan int, 1)
	n.Spawn(func(s *proc.Self) error {
		c, err := genserver.StartLink(context.Background(), s, Counter{Initial: 1})
		if err != nil {
			return err
		}
		c.Cast(s, Add{1})
		v, err := c.Call(context.Background(), s, Get{})
		if err != nil {
			return err
		}
		got <- v
		return nil
	})
	if v := <-got; v != 2 {
		t.Errorf("Get = %d", v)
	}
}

// validator replies nil errors, which arrive as untyped nil.
type validator struct{}

func (validator) Init(proc.PID) (struct{}, []gen.Effect, error) { return struct{}{}, nil, nil }

func (validator) HandleCall(s struct{}, n int, from genserver.From[error]) (struct{}, []gen.Effect) {
	if n < 0 {
		return s, gen.Do(from.Reply(errBoom))
	}
	return s, gen.Do(from.Reply(nil))
}

func (validator) HandleCast(s struct{}, _ struct{}) (struct{}, []gen.Effect) { return s, nil }

func TestNilReply(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	v, _ := genserver.Start(ctx, n, validator{})
	if rep, err := v.Call(ctx, n, 1); rep != nil || err != nil {
		t.Errorf("Call(1) = %v, %v; want a nil reply", rep, err)
	}
	if rep, err := v.Call(ctx, n, -1); rep != errBoom || err != nil {
		t.Errorf("Call(-1) = %v, %v", rep, err)
	}
}

// warmer finishes its initialization in HandleContinue, after Start has
// returned, and knows its own PID.
type warmer struct{}

type warmState struct {
	self   proc.PID
	warmed bool
}

func (warmer) Init(self proc.PID) (warmState, []gen.Effect, error) {
	return warmState{self: self}, gen.Do(gen.Continue{Msg: "warm up"}), nil
}

func (warmer) HandleContinue(s warmState, msg any) (warmState, []gen.Effect) {
	s.warmed = msg == "warm up"
	return s, nil
}

func (warmer) HandleCall(s warmState, _ struct{}, from genserver.From[warmState]) (warmState, []gen.Effect) {
	return s, gen.Do(from.Reply(s))
}

func (warmer) HandleCast(s warmState, _ struct{}) (warmState, []gen.Effect) { return s, nil }

func TestHandleContinue(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	w, err := genserver.Start(ctx, n, warmer{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := w.Call(ctx, n, struct{}{})
	if err != nil || !s.warmed || s.self != w.Dest() {
		t.Errorf("state = %+v, %v; want warmed, with self %v", s, err, w.Dest())
	}
}

// cold returns gen.Continue without a HandleContinue.
type cold struct{}

func (cold) Init(proc.PID) (int, []gen.Effect, error) {
	return 0, gen.Do(gen.Continue{Msg: "oops"}), nil
}

func (cold) HandleCall(n int, _ struct{}, _ genserver.From[int]) (int, []gen.Effect) { return n, nil }
func (cold) HandleCast(n int, _ struct{}) (int, []gen.Effect)                        { return n, nil }

func TestNoHandleContinue(t *testing.T) {
	n := proc.NewNode("")
	reasons := make(chan error, 1)
	// The server stops right after starting: learn of it through the link
	// of a parent trapping exits, which cannot miss it.
	n.Spawn(func(s *proc.Self) error {
		s.TrapExit(true)
		if _, err := genserver.StartLink(context.Background(), s, cold{}); err != nil {
			return err
		}
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		reasons <- msg.(proc.ExitMsg).Reason
		return nil
	})
	if r := <-reasons; r != genserver.ErrNoHandleContinue {
		t.Errorf("exit reason = %v", r)
	}
}

func TestRefStop(t *testing.T) {
	rec := make(chan any, 1)
	n := proc.NewNode("")
	ctx := context.Background()
	observer := n.Spawn(func(s *proc.Self) error {
		msg, err := s.Receive(ctx)
		rec <- msg
		return err
	})
	e, err := genserver.Start(ctx, n, echo{observer})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Stop(ctx, n); err != nil {
		t.Fatal(err)
	}
	if got := <-rec; got != (stopped{proc.Normal}) {
		t.Errorf("Terminate got %#v", got)
	}
}

// logServer keeps what it is sent, its other callbacks by Default.
type logServer struct{ genserver.Default[[]string] }

func (logServer) HandleInfo(log []string, msg any) ([]string, []gen.Effect) {
	return append(log[:len(log):len(log)], fmt.Sprint(msg)), nil
}

// castOnly takes casts, and no calls.
type castOnly struct{ genserver.Default[int] }

func (castOnly) HandleCast(n int, by int) (int, []gen.Effect) { return n + by, nil }

func TestDefault(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	l, err := genserver.Start(ctx, n, logServer{})
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := l.Dest().WhereIs(n)
	n.Send(pid, "hello")
	if s, err := gen.GetState(ctx, n, pid); err != nil || !reflect.DeepEqual(s, []string{"hello"}) {
		t.Errorf("state %v, %v", s, err)
	}
	// It takes no call: one stops it.
	if _, err := gen.Call(ctx, n, pid, "call"); err == nil {
		t.Error("call answered")
	}

	c, err := genserver.Start(ctx, n, castOnly{})
	if err != nil {
		t.Fatal(err)
	}
	c.Cast(n, 2)
	cpid, _ := c.Dest().WhereIs(n)
	if s, err := gen.GetState(ctx, n, cpid); err != nil || s != 2 {
		t.Errorf("state %v, %v", s, err)
	}
}
