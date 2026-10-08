package genserver_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/internal/testlog"
	"github.com/shun159/molecule/proc"
)

var errBoom = errors.New("boom")

func TestCounterPure(t *testing.T) {
	var from genserver.From[int]
	n, effs := Counter{}.HandleCall(3, Reset{}, from)
	if n != 0 || !reflect.DeepEqual(effs, molecule.Do(molecule.Reply{To: from.From, Value: 3})) {
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

func (e echo) Init(proc.PID) (echoState, []molecule.Effect, error) {
	return echoState{observer: e.observer}, nil, nil
}

func (echo) HandleCall(s echoState, req string, from genserver.From[string]) (echoState, []molecule.Effect) {
	if req == "stop" {
		return s, molecule.Do(from.Reply("bye"), molecule.Stop{Reason: errBoom})
	}
	return s, molecule.Do(from.Reply(req))
}

func (echo) HandleCast(s echoState, _ struct{}) (echoState, []molecule.Effect) { return s, nil }

func (echo) HandleInfo(s echoState, msg any) (echoState, []molecule.Effect) {
	s.infos = append(s.infos[:len(s.infos):len(s.infos)], msg)
	return s, nil
}

func (echo) Terminate(s echoState, reason error) []molecule.Effect {
	return molecule.Do(molecule.Send{To: s.observer, Msg: stopped{reason}})
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
	for _, send := range []func(molecule.Dest){
		func(d molecule.Dest) {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			molecule.Call(ctx, n, d, "not a CounterReq")
		},
		func(d molecule.Dest) { molecule.SendCast(n, d, "not an Add") },
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
		molecule.SendReply(s, msg.(molecule.CallMsg).From, "not an int")
		return nil
	})
	_, err := genserver.NewRef[CounterReq, int, Add](pid).Call(context.Background(), n, Get{})
	if err == nil {
		t.Error("Call accepted a reply of the wrong type")
	}
}

func TestCallErrors(t *testing.T) {
	n := proc.NewNode("")
	ref := genserver.RefFor(Counter{}, molecule.Local("nobody"))
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

func (relay) Init(proc.PID) (relayState, []molecule.Effect, error) {
	return relayState{pending: map[int]genserver.From[int]{}}, nil, nil
}

func (r relay) HandleCall(s relayState, _ Get, from genserver.From[int]) (relayState, []molecule.Effect) {
	pending := maps.Clone(s.pending)
	pending[s.next] = from
	return relayState{pending: pending, next: s.next + 1}, molecule.Do(r.counter.CallEffect(Get{}, s.next))
}

func (relay) HandleCast(s relayState, _ struct{}) (relayState, []molecule.Effect) { return s, nil }

func (relay) HandleInfo(s relayState, msg any) (relayState, []molecule.Effect) {
	resp, ok := msg.(molecule.Response)
	if !ok {
		return s, nil
	}
	from := s.pending[resp.Tag.(int)]
	pending := maps.Clone(s.pending)
	delete(pending, resp.Tag.(int))
	return relayState{pending: pending, next: s.next}, molecule.Do(from.Reply(resp.Value.(int)))
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

func (validator) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (validator) HandleCall(s struct{}, n int, from genserver.From[error]) (struct{}, []molecule.Effect) {
	if n < 0 {
		return s, molecule.Do(from.Reply(errBoom))
	}
	return s, molecule.Do(from.Reply(nil))
}

func (validator) HandleCast(s struct{}, _ struct{}) (struct{}, []molecule.Effect) { return s, nil }

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

func (warmer) Init(self proc.PID) (warmState, []molecule.Effect, error) {
	return warmState{self: self}, molecule.Do(molecule.Continue{Msg: "warm up"}), nil
}

func (warmer) HandleContinue(s warmState, msg any) (warmState, []molecule.Effect) {
	s.warmed = msg == "warm up"
	return s, nil
}

func (warmer) HandleCall(s warmState, _ struct{}, from genserver.From[warmState]) (warmState, []molecule.Effect) {
	return s, molecule.Do(from.Reply(s))
}

func (warmer) HandleCast(s warmState, _ struct{}) (warmState, []molecule.Effect) { return s, nil }

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

// cold returns molecule.Continue without a HandleContinue.
type cold struct{}

func (cold) Init(proc.PID) (int, []molecule.Effect, error) {
	return 0, molecule.Do(molecule.Continue{Msg: "oops"}), nil
}

func (cold) HandleCall(n int, _ struct{}, _ genserver.From[int]) (int, []molecule.Effect) {
	return n, nil
}
func (cold) HandleCast(n int, _ struct{}) (int, []molecule.Effect) { return n, nil }

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

func (logServer) HandleInfo(log []string, msg any) ([]string, []molecule.Effect) {
	return append(log[:len(log):len(log)], fmt.Sprint(msg)), nil
}

// castOnly takes casts, and no calls.
type castOnly struct{ genserver.Default[int] }

func (castOnly) HandleCast(n int, by int) (int, []molecule.Effect) { return n + by, nil }

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
	if _, err := molecule.Call(ctx, n, pid, "call"); err == nil {
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

// tally adds the casts it gets, and answers calls with the sum.
type tally struct{ genserver.Default[int] }

type sum struct{}

func (tally) HandleCall(n int, _ sum, from genserver.From[int]) (int, []molecule.Effect) {
	return n, molecule.Do(from.Reply(n))
}

func (tally) HandleCast(n int, by int) (int, []molecule.Effect) { return n + by, nil }

func TestSendRequest(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	ref, err := genserver.Start(ctx, n, tally{})
	if err != nil {
		t.Fatal(err)
	}
	ref.Cast(n, 3)
	p := ref.SendRequest(n, sum{})
	if v, err := p.Wait(ctx); v != 3 || err != nil {
		t.Errorf("SendRequest: %v, %v", v, err)
	}
}

// vault keeps a secret, and panics on a cast; its reports hide the secret.
type vault struct{ genserver.Default[string] }

func (vault) Init(proc.PID) (string, []molecule.Effect, error) { return "s3cr3t", nil, nil }

func (vault) HandleCast(string, struct{}) (string, []molecule.Effect) { panic("boom") }

func (vault) FormatStatus(st molecule.Status) molecule.Status {
	st.State = "redacted"
	return st
}

func TestFormatStatus(t *testing.T) {
	rec, logger := testlog.New()
	n := proc.NewNode("", proc.WithLogger(logger))
	ctx := context.Background()
	ref, err := genserver.Start(ctx, n, vault{})
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := ref.Dest().WhereIs(n)
	gone, stop := n.Watch(ctx, pid)
	defer stop()
	ref.Cast(n, struct{}{})
	<-gone.Done()
	reports := rec.Records("behaviour terminating")
	if len(reports) != 1 || reports[0].Attrs["state"] != "redacted" {
		t.Fatalf("reports %+v", reports)
	}
	for _, r := range rec.Records("") {
		for _, v := range r.Attrs {
			if strings.Contains(v, "s3cr3t") {
				t.Errorf("secret logged: %+v", r)
			}
		}
	}
}
