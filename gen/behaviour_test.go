package gen_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/internal/testlog"
	"github.com/shun159/molecule/proc"
)

// counter is a minimal behaviour, used to show that behaviours are tested
// as plain functions.
type counter struct{}

type (
	incr  struct{ by int }
	get   struct{}
	reset struct{}
)

func (counter) Init(_ proc.PID, args any) (int, []gen.Effect, error) { return args.(int), nil, nil }

func (counter) Handle(n int, msg gen.Msg) (int, []gen.Effect) {
	switch m := msg.(type) {
	case gen.CallMsg:
		switch m.Req.(type) {
		case get:
			return n, gen.Do(gen.Reply{To: m.From, Value: n})
		case reset:
			return 0, gen.Do(gen.Reply{To: m.From, Value: n}, gen.Stop{})
		}
	case gen.CastMsg:
		if r, ok := m.Req.(incr); ok {
			return n + r.by, nil
		}
	}
	return n, nil
}

func (counter) Terminate(int, error) []gen.Effect { return nil }

func TestCounterPure(t *testing.T) {
	from := gen.From{}
	for _, tt := range []struct {
		state int
		msg   gen.Msg
		want  int
		effs  []gen.Effect
	}{
		{1, gen.CastMsg{Req: incr{2}}, 3, nil},
		{3, gen.CallMsg{From: from, Req: get{}}, 3, gen.Do(gen.Reply{To: from, Value: 3})},
		{3, gen.CallMsg{From: from, Req: reset{}}, 0, gen.Do(gen.Reply{To: from, Value: 3}, gen.Stop{})},
		{3, gen.InfoMsg{Msg: "noise"}, 3, nil},
	} {
		got, effs := counter{}.Handle(tt.state, tt.msg)
		if got != tt.want || !reflect.DeepEqual(effs, tt.effs) {
			t.Errorf("Handle(%d, %#v) = %d, %#v; want %d, %#v", tt.state, tt.msg, got, effs, tt.want, tt.effs)
		}
	}
}

func TestCounterRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := proc.NewNode("")
		pid, err := gen.Start(context.Background(), n, counter{}, 10)
		if err != nil {
			t.Fatal(err)
		}
		gen.SendCast(n, pid, incr{5})
		if v := call(t, n, pid, get{}); v != 15 {
			t.Errorf("get = %v", v)
		}
		if s, err := gen.GetState(context.Background(), n, pid); s != 15 || err != nil {
			t.Errorf("GetState = %v, %v", s, err)
		}
		ctx, stop := n.Watch(context.Background(), pid)
		defer stop()
		if v := call(t, n, pid, reset{}); v != 15 {
			t.Errorf("reset = %v", v)
		}
		<-ctx.Done()
		if c := context.Cause(ctx); c != proc.Normal {
			t.Errorf("exit reason = %v", c)
		}
	})
}

// puppet is a behaviour driven by its requests: a do request carries the
// effects to return. It logs every InfoMsg into its state, and reports
// Terminate to an observer.
type puppet struct{}

type pstate struct {
	n        int
	log      []any
	observer proc.PID
	self     proc.PID
}

// continued is how the puppet logs a ContinueMsg.
type continued struct{ msg any }

type pargs struct {
	observer proc.PID
	effs     []gen.Effect
	err      error
	panic    bool
}

// do asks the puppet to return effs, then reply "ok".
type do struct{ effs []gen.Effect }

type terminated struct {
	n      int
	reason error
}

func (puppet) Init(self proc.PID, args any) (pstate, []gen.Effect, error) {
	a := args.(pargs)
	if a.panic {
		panic("in init")
	}
	return pstate{observer: a.observer, self: self}, a.effs, a.err
}

func (puppet) Handle(s pstate, msg gen.Msg) (pstate, []gen.Effect) {
	switch m := msg.(type) {
	case gen.CallMsg:
		switch req := m.Req.(type) {
		case do:
			return s, append(slices.Clip(req.effs), gen.Reply{To: m.From, Value: "ok"})
		case get:
			return s, gen.Do(gen.Reply{To: m.From, Value: s.n})
		case string:
			if req == "panic" {
				panic("in handle")
			}
		}
	case gen.CastMsg:
		switch req := m.Req.(type) {
		case int:
			s.n += req
		case do:
			return s, req.effs
		}
	case gen.InfoMsg:
		s.log = append(slices.Clip(s.log), m.Msg)
	case gen.ContinueMsg:
		s.log = append(slices.Clip(s.log), continued{m.Msg})
		if d, ok := m.Msg.(do); ok {
			return s, d.effs
		}
	}
	return s, nil
}

func (puppet) Terminate(s pstate, reason error) []gen.Effect {
	return gen.Do(gen.Send{To: s.observer, Msg: terminated{s.n, reason}})
}

// collector spawns a process that forwards what it receives to a channel.
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

func kill(n *proc.Node, pid proc.PID, reason error) {
	ctx, stop := n.Watch(context.Background(), pid)
	defer stop()
	n.Spawn(func(s *proc.Self) error { s.Exit(pid, reason); return nil })
	<-ctx.Done()
}

// world is a synctest bubble with a node, an observer, and cleanup of the
// listed processes so the bubble can end.
type world struct {
	t        *testing.T
	n        *proc.Node
	observer proc.PID
	events   <-chan any
	procs    []proc.PID
}

func inWorld(t *testing.T, f func(w *world)) {
	synctest.Test(t, func(t *testing.T) {
		w := &world{t: t, n: proc.NewNode("")}
		w.observer, w.events = collector(w.n)
		w.procs = append(w.procs, w.observer)
		f(w)
		for _, pid := range w.procs {
			kill(w.n, pid, proc.Kill)
		}
	})
}

func (w *world) start(effs ...gen.Effect) proc.PID {
	w.t.Helper()
	pid, err := gen.Start(context.Background(), w.n, puppet{}, pargs{observer: w.observer, effs: effs})
	if err != nil {
		w.t.Fatal(err)
	}
	w.procs = append(w.procs, pid)
	return pid
}

func (w *world) spawn(fn func(*proc.Self) error) proc.PID {
	pid := w.n.Spawn(fn)
	w.procs = append(w.procs, pid)
	return pid
}

func (w *world) do(pid proc.PID, effs ...gen.Effect) {
	w.t.Helper()
	if v := call(w.t, w.n, pid, do{effs}); v != "ok" {
		w.t.Fatalf("do = %v", v)
	}
}

func (w *world) state(pid proc.PID) pstate {
	w.t.Helper()
	s, err := gen.GetState(context.Background(), w.n, pid)
	if err != nil {
		w.t.Fatal(err)
	}
	return s.(pstate)
}

func (w *world) log(pid proc.PID) []any {
	w.t.Helper()
	synctest.Wait()
	return w.state(pid).log
}

func (w *world) noEvent() {
	w.t.Helper()
	synctest.Wait()
	select {
	case e := <-w.events:
		w.t.Errorf("unexpected event %#v", e)
	default:
	}
}

func waitForever(s *proc.Self) error {
	for {
		if _, err := s.Receive(context.Background()); err != nil {
			return err
		}
	}
}

func TestInitFailure(t *testing.T) {
	errInit := errors.New("init failed")
	inWorld(t, func(w *world) {
		if _, err := gen.Start(context.Background(), w.n, puppet{}, pargs{err: errInit}); err != errInit {
			t.Errorf("Start = %v, want %v", err, errInit)
		}
		if _, err := gen.Start(context.Background(), w.n, puppet{}, pargs{err: gen.ErrIgnore}); err != gen.ErrIgnore {
			t.Errorf("Start = %v, want ignore", err)
		}
		var pe *proc.PanicError
		if _, err := gen.Start(context.Background(), w.n, puppet{}, pargs{panic: true}); !errors.As(err, &pe) {
			t.Errorf("Start = %v, want a panic", err)
		}
		if _, err := gen.Start(context.Background(), w.n, puppet{}, pargs{effs: gen.Do(gen.Stop{Reason: errBoom})}); err != errBoom {
			t.Errorf("Start with Stop in Init = %v", err)
		}
	})
}

func TestIgnoreDoesNotKillCaller(t *testing.T) {
	inWorld(t, func(w *world) {
		errc := make(chan error, 1)
		caller := w.spawn(func(s *proc.Self) error {
			_, err := gen.StartLink(context.Background(), s, puppet{}, pargs{err: gen.ErrIgnore})
			errc <- err
			return waitForever(s)
		})
		if err := <-errc; err != gen.ErrIgnore {
			t.Errorf("StartLink = %v", err)
		}
		synctest.Wait()
		if !w.n.IsAlive(caller) {
			t.Error("ignore killed the linked caller")
		}
	})
}

func TestStartWithName(t *testing.T) {
	inWorld(t, func(w *world) {
		name := gen.Local("srv")
		pid, err := gen.Start(context.Background(), w.n, puppet{}, pargs{observer: w.observer}, gen.WithName(name))
		if err != nil {
			t.Fatal(err)
		}
		w.procs = append(w.procs, pid)
		if got, ok := name.WhereIs(w.n); !ok || got != pid {
			t.Errorf("WhereIs = %v, %v", got, ok)
		}
		gen.SendCast(w.n, name, 4)
		if v := call(t, w.n, name, get{}); v != 4 {
			t.Errorf("get via name = %v", v)
		}

		_, err = gen.Start(context.Background(), w.n, puppet{}, pargs{}, gen.WithName(name))
		var already *gen.AlreadyStartedError
		if !errors.As(err, &already) || already.PID != pid {
			t.Errorf("second Start = %v, want already started as %v", err, pid)
		}
	})
}

func TestStopEffect(t *testing.T) {
	for _, tt := range []struct {
		reason, want error
	}{{errBoom, errBoom}, {nil, proc.Normal}} {
		inWorld(t, func(w *world) {
			pid := w.start()
			gen.SendCast(w.n, pid, 7)
			ctx, stop := w.n.Watch(context.Background(), pid)
			defer stop()

			// The reply after Stop is still sent: all effects run.
			w.do(pid, gen.Stop{Reason: tt.reason})
			if e := <-w.events; e != (terminated{7, tt.want}) {
				t.Errorf("Terminate got %#v", e)
			}
			<-ctx.Done()
			if c := context.Cause(ctx); c != tt.want {
				t.Errorf("exit reason = %v, want %v", c, tt.want)
			}
		})
	}
}

func TestHandlePanic(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		gen.SendCast(w.n, pid, 5)
		_, err := gen.Call(context.Background(), w.n, pid, "panic")
		var pe *proc.PanicError
		if !errors.As(err, &pe) {
			t.Errorf("Call = %v, want the panic as exit reason", err)
		}
		e := (<-w.events).(terminated)
		if e.n != 5 || !errors.As(e.reason, &pe) {
			t.Errorf("Terminate got %#v", e)
		}
	})
}

func TestParentExit(t *testing.T) {
	for _, trap := range []bool{true, false} {
		inWorld(t, func(w *world) {
			children := make(chan proc.PID, 1)
			parent := w.spawn(func(s *proc.Self) error {
				pid, err := gen.StartLink(context.Background(), s, puppet{},
					pargs{observer: w.observer, effs: gen.Do(gen.TrapExit{On: trap})})
				if err != nil {
					return err
				}
				children <- pid
				return waitForever(s)
			})
			child := <-children
			ctx, stop := w.n.Watch(context.Background(), child)
			defer stop()

			kill(w.n, parent, proc.Shutdown)
			<-ctx.Done()
			if c := context.Cause(ctx); c != proc.Shutdown {
				t.Errorf("trap=%v: child exit reason = %v", trap, c)
			}
			if trap {
				if e := <-w.events; e != (terminated{0, proc.Shutdown}) {
					t.Errorf("Terminate got %#v", e)
				}
			} else {
				w.noEvent() // no Terminate without trapping exits
			}
		})
	}
}

func TestKilledNoTerminate(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		kill(w.n, pid, proc.Kill)
		w.noEvent()
	})
}

func TestMonitorEffect(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		a := w.spawn(waitForever)
		b := w.spawn(waitForever)
		c := w.spawn(waitForever)

		w.do(pid,
			gen.Monitor{Target: a, Tag: "a"},
			gen.Monitor{Target: b, Tag: "b"},
			gen.Demonitor{Tag: "b"},
			gen.Monitor{Target: gen.Local("nobody"), Tag: "nobody"},
			gen.Monitor{Target: c, Tag: "a"}, // replaces the monitor of a
		)
		kill(w.n, a, errBoom)
		kill(w.n, b, errBoom)
		kill(w.n, c, errBoom)

		want := []any{
			gen.Down{Tag: "nobody", Reason: proc.NoProc},
			gen.Down{Tag: "a", PID: c, Reason: errBoom},
		}
		if got := w.log(pid); !reflect.DeepEqual(got, want) {
			t.Errorf("log = %#v\nwant %#v", got, want)
		}
	})
}

func TestTimers(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		start := time.Now()
		w.do(pid,
			gen.StartTimer{Key: "once", After: time.Second, Msg: "once"},
			gen.StartTimer{Key: "cancelled", After: time.Second, Msg: "cancelled"},
			gen.CancelTimer{Key: "cancelled"},
			gen.StartTimer{Key: "replaced", After: time.Second, Msg: "old"},
		)
		time.Sleep(500 * time.Millisecond)
		w.do(pid, gen.StartTimer{Key: "replaced", After: time.Second, Msg: "new"})

		time.Sleep(time.Until(start.Add(time.Second)) - time.Nanosecond)
		if got := w.log(pid); len(got) != 0 {
			t.Errorf("timers fired early: %v", got)
		}
		time.Sleep(time.Nanosecond)
		if got := w.log(pid); !reflect.DeepEqual(got, []any{"once"}) {
			t.Errorf("log at 1s = %v", got)
		}
		time.Sleep(time.Second)
		if got := w.log(pid); !reflect.DeepEqual(got, []any{"once", "new"}) {
			t.Errorf("log at 2s = %v", got)
		}
	})
}

// TestTimerFiredThenCancelled cancels a timer whose message is already in
// the mailbox. The message must not reach the behaviour.
func TestTimerFiredThenCancelled(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		w.do(pid,
			gen.StartTimer{Key: "cancelled", After: time.Second, Msg: "cancelled"},
			gen.StartTimer{Key: "replaced", After: time.Second, Msg: "old"},
		)
		ctx := context.Background()
		if err := gen.Suspend(ctx, w.n, pid); err != nil {
			t.Fatal(err)
		}
		// Queued ahead of the timer messages, handled after they fired.
		gen.SendCast(w.n, pid, do{gen.Do(
			gen.CancelTimer{Key: "cancelled"},
			gen.StartTimer{Key: "replaced", After: time.Second, Msg: "new"},
		)})
		time.Sleep(time.Second)
		if err := gen.Resume(ctx, w.n, pid); err != nil {
			t.Fatal(err)
		}
		if got := w.log(pid); len(got) != 0 {
			t.Errorf("stale timer messages delivered: %v", got)
		}
		time.Sleep(time.Second)
		if got := w.log(pid); !reflect.DeepEqual(got, []any{"new"}) {
			t.Errorf("log = %v", got)
		}
	})
}

func TestSendRequest(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		server := w.start()
		gen.SendCast(w.n, server, 42)
		gen.SendCast(w.n, pid, 7)
		dead := w.n.Spawn(func(*proc.Self) error { return nil })
		mute := w.spawn(waitForever)

		w.do(pid,
			gen.SendRequest{To: server, Req: get{}, Tag: "server"},
			gen.SendRequest{To: pid, Req: get{}, Tag: "self"},
		)
		synctest.Wait()
		w.do(pid,
			gen.SendRequest{To: gen.Local("nobody"), Req: get{}, Tag: "nobody"},
			gen.SendRequest{To: mute, Req: get{}, Tag: "mute", Timeout: time.Second},
		)
		time.Sleep(time.Second)
		w.do(pid, gen.SendRequest{To: dead, Req: get{}, Tag: "dead"})

		got := map[any]gen.Response{}
		for _, e := range w.log(pid) {
			r := e.(gen.Response)
			got[r.Tag] = r
		}
		if r := got["server"]; r.Value != 42 || r.Err != nil {
			t.Errorf("server: %#v", r)
		}
		if r := got["self"]; r.Value != 7 || r.Err != nil {
			t.Errorf("self: %#v", r)
		}
		if r := got["nobody"]; !errors.Is(r.Err, proc.NoProc) {
			t.Errorf("nobody: %#v", r)
		}
		if r := got["mute"]; r.Err != context.DeadlineExceeded {
			t.Errorf("mute: %#v", r)
		}
		if r := got["dead"]; !errors.Is(r.Err, proc.NoProc) {
			t.Errorf("dead: %#v", r)
		}
		if len(got) != 5 {
			t.Errorf("responses = %#v", got)
		}
	})
}

func TestSuspendResume(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		ctx := context.Background()
		if err := gen.Suspend(ctx, w.n, pid); err != nil {
			t.Fatal(err)
		}
		gen.SendCast(w.n, pid, 1)
		w.n.Send(pid, "info")
		gen.SendCast(w.n, pid, 2)
		if s := w.state(pid); s.n != 0 || len(s.log) != 0 {
			t.Errorf("handled while suspended: %+v", s)
		}
		if err := gen.Resume(ctx, w.n, pid); err != nil {
			t.Fatal(err)
		}
		if s := w.state(pid); s.n != 3 || !reflect.DeepEqual(s.log, []any{"info"}) {
			t.Errorf("after resume: %+v", s)
		}
	})
}

func TestLinkEffects(t *testing.T) {
	inWorld(t, func(w *world) {
		trapping := w.start(gen.TrapExit{On: true})
		plain := w.start()
		other := w.spawn(waitForever)
		w.do(trapping, gen.Link{PID: other})
		w.do(plain, gen.Link{PID: other})

		ctx, stop := w.n.Watch(context.Background(), plain)
		defer stop()
		kill(w.n, other, errBoom)
		<-ctx.Done()
		if c := context.Cause(ctx); c != errBoom {
			t.Errorf("linked behaviour exit reason = %v", c)
		}
		want := []any{proc.ExitMsg{From: other, Reason: errBoom}}
		if got := w.log(trapping); !reflect.DeepEqual(got, want) {
			t.Errorf("trapping log = %#v", got)
		}

		unlinked := w.start()
		another := w.spawn(waitForever)
		w.do(unlinked, gen.Link{PID: another}, gen.Unlink{PID: another})
		kill(w.n, another, errBoom)
		if !w.n.IsAlive(unlinked) {
			t.Error("unlinked behaviour died")
		}
	})
}

func TestSendAndCastEffects(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		peer := w.start()
		w.do(pid,
			gen.Send{To: w.observer, Msg: "hello"},
			gen.Cast{To: peer, Req: 3},
			gen.Send{To: gen.Local("nobody"), Msg: "dropped"},
		)
		if e := <-w.events; e != "hello" {
			t.Errorf("observer got %#v", e)
		}
		if v := call(t, w.n, peer, get{}); v != 3 {
			t.Errorf("peer state = %v", v)
		}
	})
}

// TestRemoteDest sends effects to a Remote name, here of the node itself:
// the runtime sends them by name.
func TestRemoteDest(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		peer := w.start()
		w.n.Register("observer", w.observer)
		w.n.Register("peer", peer)
		w.do(pid,
			gen.Send{To: gen.Remote{Node: w.n.Name(), Name: "observer"}, Msg: "by name"},
			gen.Cast{To: gen.Remote{Node: w.n.Name(), Name: "peer"}, Req: 7},
			gen.Send{To: gen.Remote{Node: "elsewhere", Name: "observer"}, Msg: "lost"},
		)
		if e := <-w.events; e != "by name" {
			t.Errorf("observer got %#v", e)
		}
		if v := call(t, w.n, peer, get{}); v != 7 {
			t.Errorf("peer state = %v", v)
		}
		if v, err := gen.Call(context.Background(), w.n, gen.Remote{Node: w.n.Name(), Name: "peer"}, get{}); err != nil || v != 7 {
			t.Errorf("call by remote name = %v, %v", v, err)
		}
		w.noEvent()
	})
}

func TestTerminateReport(t *testing.T) {
	rec, logger := testlog.New()
	n := proc.NewNode("", proc.WithLogger(logger))
	ctx := context.Background()

	pid, err := gen.Start(ctx, n, puppet{}, pargs{})
	if err != nil {
		t.Fatal(err)
	}
	gen.SendCast(n, pid, 5)
	down, stop := n.Watch(ctx, pid)
	defer stop()
	gen.Call(ctx, n, pid, "panic")
	<-down.Done()

	reports := rec.Records("behaviour terminating")
	if len(reports) != 1 {
		t.Fatalf("reports = %+v", rec.Records(""))
	}
	r := reports[0].Attrs
	if !strings.Contains(r["last_message"], "panic") || !strings.Contains(r["state"], "n:5") ||
		!strings.Contains(r["reason"], "in handle") || !strings.Contains(r["behaviour"], "puppet") {
		t.Errorf("report = %+v", r)
	}
	if len(rec.Records("crash report")) != 1 {
		t.Errorf("crash reports = %+v", rec.Records("crash report"))
	}

	// A normal stop is not reported.
	pid, _ = gen.Start(ctx, n, puppet{}, pargs{})
	down2, stop2 := n.Watch(ctx, pid)
	defer stop2()
	gen.Call(ctx, n, pid, do{gen.Do(gen.Stop{})})
	<-down2.Done()
	if len(rec.Records("behaviour terminating")) != 1 {
		t.Errorf("normal stop reported: %+v", rec.Records(""))
	}
}

func TestInitSelf(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		if self := w.state(pid).self; self != pid {
			t.Errorf("Init got self %v, want %v", self, pid)
		}
	})
}

// TestContinueOrder checks that continues come before the messages already
// in the mailbox, in the order returned, those returned while handling a
// continue included.
func TestContinueOrder(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		ctx := context.Background()
		if err := gen.Suspend(ctx, w.n, pid); err != nil {
			t.Fatal(err)
		}
		nested := do{gen.Do(gen.Continue{Msg: "c3"})}
		gen.SendCast(w.n, pid, do{gen.Do(
			gen.Continue{Msg: "c1"},
			gen.Continue{Msg: nested},
			gen.Continue{Msg: "c2"},
		)})
		w.n.Send(pid, "mailbox")
		if err := gen.Resume(ctx, w.n, pid); err != nil {
			t.Fatal(err)
		}
		want := []any{continued{"c1"}, continued{nested}, continued{"c2"}, continued{"c3"}, "mailbox"}
		if got := w.log(pid); !reflect.DeepEqual(got, want) {
			t.Errorf("log = %#v\nwant %#v", got, want)
		}
	})
}

func TestContinueFromInit(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start(gen.Continue{Msg: "boot"})
		w.n.Send(pid, "first")
		if got := w.log(pid); !reflect.DeepEqual(got, []any{continued{"boot"}, "first"}) {
			t.Errorf("log = %#v", got)
		}
	})
}

func TestStopDropsContinue(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		leak := do{gen.Do(gen.Send{To: w.observer, Msg: "continued after stop"})}
		w.do(pid, gen.Continue{Msg: leak}, gen.Stop{})
		if e := <-w.events; e != (terminated{0, proc.Normal}) {
			t.Errorf("event %#v", e)
		}
		w.noEvent()
	})
}

// widget implements Extension effects as an unknown behaviour's would;
// the runtime must refuse them.
type widget struct{ gen.Extension }

func TestExtensionEffectRefused(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		down, stop := w.n.Watch(context.Background(), pid)
		defer stop()
		gen.SendCast(w.n, pid, do{gen.Do(widget{})})
		<-down.Done()
		var pe *proc.PanicError
		if !errors.As(context.Cause(down), &pe) || !strings.Contains(fmt.Sprint(pe.Value), "widget") {
			t.Errorf("exit reason = %v", context.Cause(down))
		}
	})
}

// stamp is a Performer sending its message from the process performing
// it, through the Env.
type stamp struct {
	gen.Extension
	to  proc.PID
	msg string
}

func (e stamp) Perform(env gen.Env) { env.Send(e.to, e.msg+" from "+env.Self().String()) }

func TestPerformer(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		w.do(pid,
			gen.Send{To: w.observer, Msg: "first"},
			stamp{to: w.observer, msg: "second"},
			gen.Send{To: w.observer, Msg: "third"},
		)
		for _, want := range []string{"first", "second from " + pid.String(), "third"} {
			if e := <-w.events; e != want {
				t.Errorf("observer got %#v, want %q", e, want)
			}
		}
	})
}

func TestTerminate(t *testing.T) {
	for _, reason := range []error{nil, errBoom} {
		inWorld(t, func(w *world) {
			pid := w.start()
			gen.SendCast(w.n, pid, 3)
			if err := gen.Terminate(context.Background(), w.n, pid, reason); err != nil {
				t.Fatalf("Terminate(%v) = %v", reason, err)
			}
			want := reason
			if want == nil {
				want = proc.Normal
			}
			if e := <-w.events; e != (terminated{3, want}) {
				t.Errorf("Terminate callback got %#v", e)
			}
			if w.n.IsAlive(pid) {
				t.Error("alive after Terminate returned")
			}
		})
	}
}

func TestTerminateSuspended(t *testing.T) {
	inWorld(t, func(w *world) {
		pid := w.start()
		ctx := context.Background()
		gen.Suspend(ctx, w.n, pid)
		if err := gen.Terminate(ctx, w.n, pid, nil); err != nil {
			t.Fatal(err)
		}
		<-w.events
	})
}

func TestTerminateNoProc(t *testing.T) {
	n := proc.NewNode("")
	if err := gen.Terminate(context.Background(), n, gen.Local("nobody"), nil); !errors.Is(err, proc.NoProc) {
		t.Errorf("Terminate = %v", err)
	}
}
