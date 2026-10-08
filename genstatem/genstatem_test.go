package genstatem_test

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

// lab is a machine driven by its events: a cmd says which state to go to
// and which effects to return. Its data is a log of "state:event" for
// every event it handles.
type lab struct {
	enter bool
	init  []gen.Effect
}

type cmd struct {
	label      string
	next       string // "" keeps the state
	effs       []gen.Effect
	postponeIn string // the state in which to postpone the event
}

func (l lab) StateEnter() bool { return l.enter }

func (l lab) Init(proc.PID) (string, []string, []gen.Effect, error) {
	return "idle", nil, l.init, nil
}

func (lab) HandleEvent(state string, log []string, ev genstatem.Event) (string, []string, []gen.Effect) {
	log = append(slices.Clip(log), state+":"+describe(ev))
	var c cmd
	switch e := ev.(type) {
	case genstatem.Cast:
		c, _ = e.Msg.(cmd)
	case genstatem.Internal:
		c, _ = e.Msg.(cmd)
	case genstatem.Call:
		c, _ = e.Req.(cmd)
		if c.postponeIn != state {
			c.effs = append(slices.Clip(c.effs), e.Reply(log[len(log)-1]))
		}
	}
	if c.postponeIn == state {
		c.effs = append(slices.Clip(c.effs), genstatem.Postpone{})
	}
	next := state
	if c.next != "" {
		next = c.next
	}
	return next, log, c.effs
}

func describe(ev genstatem.Event) string {
	switch e := ev.(type) {
	case genstatem.Cast:
		return "cast(" + label(e.Msg) + ")"
	case genstatem.Call:
		return "call(" + label(e.Req) + ")"
	case genstatem.Internal:
		return "internal(" + label(e.Msg) + ")"
	case genstatem.Info:
		return "info(" + label(e.Msg) + ")"
	case genstatem.Enter[string]:
		return "enter(" + e.Old + ")"
	case genstatem.StateTimeout:
		return "state_timeout(" + label(e.Msg) + ")"
	case genstatem.EventTimeout:
		return "event_timeout(" + label(e.Msg) + ")"
	case genstatem.Timeout:
		return fmt.Sprintf("timeout(%v,%s)", e.Name, label(e.Msg))
	}
	return fmt.Sprintf("%T", ev)
}

func label(v any) string {
	if c, ok := v.(cmd); ok {
		return c.label
	}
	return fmt.Sprint(v)
}

type world struct {
	t   *testing.T
	n   *proc.Node
	pid proc.PID
}

// inWorld runs f in a synctest bubble with a lab machine started.
func inWorld(t *testing.T, l lab, f func(w *world)) {
	synctest.Test(t, func(t *testing.T) {
		n := proc.NewNode("")
		pid, err := genstatem.Start(context.Background(), n, l)
		if err != nil {
			t.Fatal(err)
		}
		w := &world{t: t, n: n, pid: pid}
		f(w)
		ctx, stop := n.Watch(context.Background(), pid)
		defer stop()
		n.Spawn(func(s *proc.Self) error { s.Exit(pid, proc.Kill); return nil })
		<-ctx.Done()
	})
}

func (w *world) cast(c cmd) { gen.SendCast(w.n, w.pid, c) }

func (w *world) log() []string {
	w.t.Helper()
	synctest.Wait()
	_, log, err := genstatem.GetState[string, []string](context.Background(), w.n, w.pid)
	if err != nil {
		w.t.Fatal(err)
	}
	return log
}

func (w *world) state() string {
	w.t.Helper()
	state, _, err := genstatem.GetState[string, []string](context.Background(), w.n, w.pid)
	if err != nil {
		w.t.Fatal(err)
	}
	return state
}

func (w *world) expect(want ...string) {
	w.t.Helper()
	if got := w.log(); !reflect.DeepEqual(got, want) {
		w.t.Errorf("log = %q\nwant  %q", got, want)
	}
}

func TestStateChangeAndEnter(t *testing.T) {
	inWorld(t, lab{enter: true}, func(w *world) {
		w.expect("idle:enter(idle)")
		w.cast(cmd{label: "a", next: "busy"})
		w.cast(cmd{label: "b"})
		w.cast(cmd{label: "c", next: "busy"}) // same state: no enter
		w.expect("idle:enter(idle)", "idle:cast(a)", "busy:enter(idle)", "busy:cast(b)", "busy:cast(c)")
		if s := w.state(); s != "busy" {
			t.Errorf("state = %q", s)
		}
	})
}

func TestNoEnterByDefault(t *testing.T) {
	inWorld(t, lab{}, func(w *world) {
		w.cast(cmd{label: "a", next: "busy"})
		w.expect("idle:cast(a)")
	})
}

// TestPostpone postpones a call until the state changes: its caller waits,
// then gets the answer from the new state.
func TestPostpone(t *testing.T) {
	inWorld(t, lab{}, func(w *world) {
		reply := make(chan any, 1)
		go func() {
			v, _ := gen.Call(context.Background(), w.n, w.pid, cmd{label: "x", postponeIn: "idle"})
			reply <- v
		}()
		synctest.Wait() // the call is in first
		w.cast(cmd{label: "y", postponeIn: "idle"})
		synctest.Wait()
		w.cast(cmd{label: "stay"}) // no state change: still postponed
		w.expect("idle:call(x)", "idle:cast(y)", "idle:cast(stay)")
		select {
		case v := <-reply:
			t.Fatalf("postponed call answered: %v", v)
		default:
		}

		w.cast(cmd{label: "go", next: "busy"})
		w.expect("idle:call(x)", "idle:cast(y)", "idle:cast(stay)", "idle:cast(go)", "busy:call(x)", "busy:cast(y)")
		if v := <-reply; v != "busy:call(x)" {
			t.Errorf("reply = %v", v)
		}
	})
}

// TestNextEventOrder inserts events: each comes before the events already
// queued, and all before the mailbox.
func TestNextEventOrder(t *testing.T) {
	inWorld(t, lab{}, func(w *world) {
		ctx := context.Background()
		gen.Suspend(ctx, w.n, w.pid)
		d := cmd{label: "d"}
		b := cmd{label: "b", effs: gen.Do(genstatem.NextEvent{Event: genstatem.Internal{Msg: d}})}
		c := cmd{label: "c"}
		w.cast(cmd{label: "a", effs: gen.Do(
			genstatem.NextEvent{Event: genstatem.Internal{Msg: b}},
			genstatem.NextEvent{Event: genstatem.Internal{Msg: c}},
		)})
		w.n.Send(w.pid, "mail")
		gen.Resume(ctx, w.n, w.pid)
		w.expect("idle:cast(a)", "idle:internal(b)", "idle:internal(d)", "idle:internal(c)", "idle:info(mail)")
	})
}

func TestPostponedAfterInserted(t *testing.T) {
	inWorld(t, lab{enter: true}, func(w *world) {
		w.cast(cmd{label: "p", postponeIn: "idle"})
		w.cast(cmd{label: "go", next: "busy", effs: gen.Do(genstatem.NextEvent{Event: genstatem.Internal{Msg: cmd{label: "n"}}})})
		w.expect("idle:enter(idle)", "idle:cast(p)", "idle:cast(go)", "busy:enter(idle)", "busy:internal(n)", "busy:cast(p)")
	})
}

func TestStateTimeout(t *testing.T) {
	inWorld(t, lab{}, func(w *world) {
		start := time.Now()
		w.cast(cmd{label: "arm", effs: gen.Do(genstatem.StartStateTimeout{After: time.Second, Msg: "t1"})})
		time.Sleep(time.Second - time.Nanosecond)
		w.expect("idle:cast(arm)")
		time.Sleep(time.Nanosecond)
		w.expect("idle:cast(arm)", "idle:state_timeout(t1)")
		if d := time.Since(start); d != time.Second {
			t.Errorf("fired after %v", d)
		}

		// A state change cancels it...
		w.cast(cmd{label: "arm2", effs: gen.Do(genstatem.StartStateTimeout{After: time.Second, Msg: "t2"})})
		w.cast(cmd{label: "go", next: "busy"})
		time.Sleep(2 * time.Second)
		// ...but one started by the transition is for the new state.
		w.cast(cmd{label: "back", next: "idle", effs: gen.Do(genstatem.StartStateTimeout{After: time.Second, Msg: "t3"})})
		time.Sleep(time.Second)
		w.expect("idle:cast(arm)", "idle:state_timeout(t1)", "idle:cast(arm2)", "idle:cast(go)",
			"busy:cast(back)", "idle:state_timeout(t3)")

		w.cast(cmd{label: "arm4", effs: gen.Do(genstatem.StartStateTimeout{After: time.Second, Msg: "t4"})})
		w.cast(cmd{label: "cancel", effs: gen.Do(genstatem.CancelStateTimeout{})})
		time.Sleep(2 * time.Second)
		if l := w.log(); l[len(l)-1] != "idle:cast(cancel)" {
			t.Errorf("cancelled state timeout fired: %q", l)
		}
	})
}

func TestEventTimeout(t *testing.T) {
	inWorld(t, lab{}, func(w *world) {
		w.cast(cmd{label: "arm", effs: gen.Do(genstatem.StartEventTimeout{After: time.Second, Msg: "quiet"})})
		time.Sleep(500 * time.Millisecond)
		w.cast(cmd{label: "noise"}) // any event cancels it
		time.Sleep(2 * time.Second)
		w.cast(cmd{label: "rearm", effs: gen.Do(genstatem.StartEventTimeout{After: time.Second, Msg: "quiet"})})
		time.Sleep(time.Second)
		w.expect("idle:cast(arm)", "idle:cast(noise)", "idle:cast(rearm)", "idle:event_timeout(quiet)")
	})
}

func TestGenericTimeout(t *testing.T) {
	inWorld(t, lab{}, func(w *world) {
		w.cast(cmd{label: "arm", effs: gen.Do(
			genstatem.StartTimeout{Name: "a", After: time.Second, Msg: "a1"},
			genstatem.StartTimeout{Name: "b", After: time.Second, Msg: "b1"},
			genstatem.StartTimeout{Name: "c", After: time.Second, Msg: "c1"},
		)})
		w.cast(cmd{label: "go", next: "busy", effs: gen.Do( // survives the state change
			genstatem.StartTimeout{Name: "b", After: 2 * time.Second, Msg: "b2"}, // replaces b1
			genstatem.CancelTimeout{Name: "c"},
		)})
		time.Sleep(3 * time.Second)
		w.expect("idle:cast(arm)", "idle:cast(go)", "busy:timeout(a,a1)", "busy:timeout(b,b2)")
	})
}

func TestInitActions(t *testing.T) {
	l := lab{enter: true, init: gen.Do(
		genstatem.NextEvent{Event: genstatem.Internal{Msg: cmd{label: "boot"}}},
		genstatem.StartStateTimeout{After: time.Second, Msg: "init"},
	)}
	inWorld(t, l, func(w *world) {
		w.n.Send(w.pid, "mail")
		time.Sleep(time.Second)
		w.expect("idle:enter(idle)", "idle:internal(boot)", "idle:info(mail)", "idle:state_timeout(init)")
	})
}

// misbehaving stops in various wrong ways from a state enter call.
type misbehaving struct{ effs []gen.Effect }

func (m misbehaving) StateEnter() bool { return true }

func (m misbehaving) Init(proc.PID) (int, struct{}, []gen.Effect, error) {
	return 0, struct{}{}, nil, nil
}

func (m misbehaving) HandleEvent(state int, d struct{}, ev genstatem.Event) (int, struct{}, []gen.Effect) {
	switch ev.(type) {
	case genstatem.Enter[int]:
		if state == 1 {
			if m.effs == nil {
				return 2, d, nil // changes the state: not allowed
			}
			return state, d, m.effs
		}
	case genstatem.Cast:
		return 1, d, nil
	}
	return state, d, nil
}

func TestEnterMisuse(t *testing.T) {
	for _, tt := range []struct {
		effs []gen.Effect
		want error
	}{
		{nil, genstatem.ErrEnterChangedState},
		{gen.Do(genstatem.Postpone{}), genstatem.ErrEnterAction},
		{gen.Do(genstatem.NextEvent{Event: genstatem.Internal{}}), genstatem.ErrEnterAction},
	} {
		synctest.Test(t, func(t *testing.T) {
			n := proc.NewNode("")
			pid, err := genstatem.Start(context.Background(), n, misbehaving{tt.effs})
			if err != nil {
				t.Fatal(err)
			}
			down, stop := n.Watch(context.Background(), pid)
			defer stop()
			gen.SendCast(n, pid, "go")
			<-down.Done()
			if r := context.Cause(down); r != tt.want {
				t.Errorf("exit reason = %v, want %v", r, tt.want)
			}
		})
	}
}

// closer reports Terminate to an observer, with an action mixed in that
// must be ignored rather than reach the gen runtime.
type closer struct{ observer chan<- string }

func (closer) Init(proc.PID) (string, int, []gen.Effect, error) { return "open", 7, nil, nil }

func (closer) HandleEvent(state string, n int, ev genstatem.Event) (string, int, []gen.Effect) {
	if _, ok := ev.(genstatem.Cast); ok {
		return state, n, gen.Do(gen.Stop{})
	}
	return state, n, nil
}

func (c closer) Terminate(state string, n int, reason error) []gen.Effect {
	c.observer <- fmt.Sprintf("%s %d %v", state, n, reason)
	return gen.Do(genstatem.Postpone{}, genstatem.StartStateTimeout{After: time.Second})
}

func TestTerminate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := proc.NewNode("")
		got := make(chan string, 1)
		pid, err := genstatem.Start(context.Background(), n, closer{got})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := genstatem.GetState[int, int](context.Background(), n, pid); err == nil {
			t.Error("GetState with the wrong types succeeded")
		}
		down, stop := n.Watch(context.Background(), pid)
		defer stop()
		gen.SendCast(n, pid, "stop")
		<-down.Done()
		if r := context.Cause(down); r != proc.Normal {
			t.Errorf("exit reason = %v", r)
		}
		if s := <-got; s != "open 7 normal" {
			t.Errorf("Terminate got %q", s)
		}
	})
}

// TestInitEventsAfterStart has an event inserted by Init stop the machine:
// as in Erlang, Start has succeeded by then, and the machine stops after.
func TestInitEventsAfterStart(t *testing.T) {
	n := proc.NewNode("")
	stopper := cmd{label: "stop", effs: gen.Do(gen.Stop{Reason: errBoom})}
	l := lab{init: gen.Do(genstatem.NextEvent{Event: genstatem.Internal{Msg: stopper}})}
	type result struct {
		err    error
		reason error
	}
	results := make(chan result, 1)
	// The machine stops right after starting: learn of it through the
	// link of a parent trapping exits, which cannot miss it.
	n.Spawn(func(s *proc.Self) error {
		s.TrapExit(true)
		if _, err := genstatem.StartLink(context.Background(), s, l); err != nil {
			results <- result{err: err}
			return nil
		}
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		results <- result{reason: msg.(proc.ExitMsg).Reason}
		return nil
	})
	r := <-results
	if r.err != nil {
		t.Fatalf("Start = %v; want success, the inserted event coming after", r.err)
	}
	if r.reason != errBoom {
		t.Errorf("exit reason = %v", r.reason)
	}
}

var errBoom = fmt.Errorf("boom")

func TestRef(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	pid, err := genstatem.Start(ctx, n, echoMachine{})
	if err != nil {
		t.Fatal(err)
	}
	ref := genstatem.NewRef(pid)
	ref.Cast(n, "cast")
	if v, err := ref.Call(ctx, n, "call"); err != nil || v != "call" {
		t.Errorf("Call: %v, %v", v, err)
	}
	if v, err := ref.SendRequest(n, "request").Wait(ctx); err != nil || v != "request" {
		t.Errorf("SendRequest: %v, %v", v, err)
	}
}

// echoMachine answers each call with its request.
type echoMachine struct{}

func (echoMachine) Init(proc.PID) (int, int, []gen.Effect, error) { return 0, 0, nil, nil }

func (echoMachine) HandleEvent(st, d int, ev genstatem.Event) (int, int, []gen.Effect) {
	if c, ok := ev.(genstatem.Call); ok {
		return st, d, gen.Do(c.Reply(c.Req))
	}
	return st, d, nil
}
