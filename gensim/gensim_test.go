package gensim_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

// counter is a gen_server holding a number.
type counter struct{}

type (
	get   struct{}
	set   struct{ n int }
	add   struct{ n int }
	crash struct{}
	mute  struct{} // never answered
)

func (counter) Init(proc.PID) (int, []molecule.Effect, error) { return 0, nil, nil }

func (counter) HandleCall(n int, req any, from genserver.From[int]) (int, []molecule.Effect) {
	switch r := req.(type) {
	case get:
		return n, molecule.Do(from.Reply(n))
	case set:
		return r.n, molecule.Do(from.Reply(r.n))
	case crash:
		panic("crash")
	}
	return n, nil // mute: no reply
}

func (counter) HandleCast(n int, msg add) (int, []molecule.Effect) { return n + msg.n, nil }

func spawn[S any](t *testing.T, s *gensim.Sim, b gen.Behaviour[S], opts ...gensim.SpawnOption) proc.PID {
	t.Helper()
	pid, err := gensim.Spawn(s, b, nil, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func TestCallAndCast(t *testing.T) {
	s := gensim.New(1)
	c := spawn(t, s, genserver.Gen(counter{}), gensim.Named("counter"))
	s.Cast(c, add{2})
	s.Cast(molecule.Local("counter"), add{3})
	if v, err := s.Call(molecule.Local("counter"), get{}); v != 5 || err != nil {
		t.Errorf("Call = %v, %v", v, err)
	}
	if n, ok := gensim.State[int](s, c); n != 5 || !ok {
		t.Errorf("State = %v, %v", n, ok)
	}
	if _, err := gensim.Spawn(s, genserver.Gen(counter{}), nil, gensim.Named("counter")); !errors.As(err, new(*molecule.AlreadyStartedError)) {
		t.Errorf("second Spawn with the name = %v", err)
	}
	if _, err := s.Call(molecule.Local("nobody"), get{}); !errors.Is(err, proc.NoProc) {
		t.Errorf("Call to nobody = %v", err)
	}
}

func TestCallFailures(t *testing.T) {
	s := gensim.New(1)
	c := spawn(t, s, genserver.Gen(counter{}))

	start := s.Now()
	if _, err := s.Call(c, mute{}); err != context.DeadlineExceeded || s.Now().Sub(start) != gensim.DefaultCallTimeout {
		t.Errorf("Call unanswered = %v after %v", err, s.Now().Sub(start))
	}

	_, err := s.Call(c, crash{})
	var pe *proc.PanicError
	if !errors.As(err, &pe) || s.Alive(c) {
		t.Errorf("Call crashing the server = %v, alive %v", err, s.Alive(c))
	}
	if _, err := s.Call(c, get{}); !errors.Is(err, proc.NoProc) {
		t.Errorf("Call to the dead server = %v", err)
	}
}

// recorder logs the casts it gets, in order.
type recorder struct{}

func (recorder) Init(proc.PID) ([]string, []molecule.Effect, error) { return nil, nil, nil }

func (recorder) HandleCall(log []string, _ get, from genserver.From[[]string]) ([]string, []molecule.Effect) {
	return log, molecule.Do(from.Reply(log))
}

func (recorder) HandleCast(log []string, msg string) ([]string, []molecule.Effect) {
	return append(slices.Clip(log), msg), nil
}

// sender casts its label, numbered, to target on go.
type sender struct {
	target proc.PID
	label  string
}

type goNow struct{}

func (sender) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (sender) HandleCall(s struct{}, _ struct{}, _ genserver.From[struct{}]) (struct{}, []molecule.Effect) {
	return s, nil
}

func (w sender) HandleCast(s struct{}, _ goNow) (struct{}, []molecule.Effect) {
	return s, molecule.Do(
		molecule.Cast{To: w.target, Req: w.label + "1"},
		molecule.Cast{To: w.target, Req: w.label + "2"},
	)
}

// interleave runs two senders against a recorder with seed, and returns
// what the recorder got, and the trace.
func interleave(t *testing.T, seed uint64) ([]string, string) {
	s := gensim.New(seed)
	r := spawn(t, s, genserver.Gen(recorder{}))
	a := spawn(t, s, genserver.Gen(sender{r, "a"}))
	b := spawn(t, s, genserver.Gen(sender{r, "b"}))
	s.Cast(a, goNow{})
	s.Cast(b, goNow{})
	s.RunUntilIdle() // the casts are all in, whatever their order
	v, err := s.Call(r, get{})
	if err != nil {
		t.Fatal(err)
	}
	return v.([]string), s.TraceString()
}

func TestDeterminism(t *testing.T) {
	orders := map[string]bool{}
	for seed := range uint64(50) {
		log, trace := interleave(t, seed)
		if _, again := interleave(t, seed); again != trace {
			t.Fatalf("seed %d: two runs differ", seed)
		}
		// Messages from one sender keep their order...
		if slices.Index(log, "a1") > slices.Index(log, "a2") || slices.Index(log, "b1") > slices.Index(log, "b2") {
			t.Fatalf("seed %d: order of one sender broken: %v", seed, log)
		}
		orders[strings.Join(log, " ")] = true
	}
	// ...while those of different senders interleave.
	if len(orders) < 3 {
		t.Errorf("50 seeds gave only %d orders: %v", len(orders), orders)
	}
}

// light is a gen_statem on for 10 seconds after each push.
type light struct{}

func (light) Init(proc.PID) (bool, struct{}, []molecule.Effect, error) {
	return false, struct{}{}, nil, nil
}

func (light) HandleEvent(on bool, d struct{}, ev genstatem.Event) (bool, struct{}, []molecule.Effect) {
	switch ev.(type) {
	case genstatem.Cast:
		return true, d, molecule.Do(genstatem.StartStateTimeout{After: 10 * time.Second, Msg: "off"})
	case genstatem.StateTimeout:
		return false, d, nil
	}
	return on, d, nil
}

func TestTimers(t *testing.T) {
	s := gensim.New(1)
	l := spawn(t, s, genstatem.Gen(light{}))
	isOn := func() bool {
		m, _ := gensim.State[genstatem.Machine[bool, struct{}]](s, l)
		return m.State()
	}
	start := s.Now()
	s.Cast(l, "push")
	s.Advance(10*time.Second - time.Nanosecond)
	if !isOn() {
		t.Fatal("off before 10s")
	}
	s.Advance(time.Nanosecond)
	if isOn() {
		t.Fatal("still on at 10s")
	}
	if d := s.Now().Sub(start); d != 10*time.Second {
		t.Errorf("clock at %v", d)
	}
}

// waiter replies to a call after 2 seconds, with a timer.
type waiter struct{}

type waiterState struct{ from genserver.From[string] }

func (waiter) Init(proc.PID) (waiterState, []molecule.Effect, error) { return waiterState{}, nil, nil }

func (waiter) HandleCall(_ waiterState, _ get, from genserver.From[string]) (waiterState, []molecule.Effect) {
	return waiterState{from}, molecule.Do(molecule.StartTimer{Key: "t", After: 2 * time.Second, Msg: "now"})
}

func (waiter) HandleCast(s waiterState, _ struct{}) (waiterState, []molecule.Effect) { return s, nil }

func (waiter) HandleInfo(s waiterState, _ any) (waiterState, []molecule.Effect) {
	return s, molecule.Do(s.from.Reply("done"))
}

func TestCallWaitsForTimers(t *testing.T) {
	s := gensim.New(1)
	w := spawn(t, s, genserver.Gen(waiter{}))
	start := s.Now()
	if v, err := s.Call(w, get{}); v != "done" || err != nil || s.Now().Sub(start) != 2*time.Second {
		t.Errorf("Call = %v, %v after %v", v, err, s.Now().Sub(start))
	}
}

// watcher monitors a process, links to another, and logs what it learns.
type watcher struct {
	monitored, linked proc.PID
	trap              bool
}

func (w watcher) Init(proc.PID) ([]any, []molecule.Effect, error) {
	return nil, molecule.Do(
		molecule.TrapExit{On: w.trap},
		molecule.Monitor{Target: w.monitored, Tag: "m"},
		molecule.Link{PID: w.linked},
	), nil
}

func (watcher) HandleCall(log []any, _ get, from genserver.From[[]any]) ([]any, []molecule.Effect) {
	return log, molecule.Do(from.Reply(log))
}

func (watcher) HandleCast(log []any, _ struct{}) ([]any, []molecule.Effect) { return log, nil }

func (watcher) HandleInfo(log []any, msg any) ([]any, []molecule.Effect) {
	return append(slices.Clip(log), msg), nil
}

func TestExitsMonitorsAndLinks(t *testing.T) {
	boom := errors.New("boom")
	for _, trap := range []bool{false, true} {
		s := gensim.New(1)
		m := spawn(t, s, genserver.Gen(counter{}))
		l := spawn(t, s, genserver.Gen(counter{}))
		w := spawn(t, s, genserver.Gen(watcher{monitored: m, linked: l, trap: trap}))

		s.Exit(m, proc.Kill)
		s.RunUntilIdle()
		log, _ := gensim.State[[]any](s, w)
		if len(log) != 1 || log[0] != (molecule.Down{Tag: "m", PID: m, Reason: proc.Killed}) {
			t.Fatalf("trap %v: log after the monitored died: %#v", trap, log)
		}

		s.Exit(l, boom)
		s.RunUntilIdle()
		if !trap {
			if s.Alive(w) {
				t.Error("watcher survived its link dying")
			}
			continue
		}
		log, _ = gensim.State[[]any](s, w)
		if len(log) != 2 || log[1] != (proc.ExitMsg{From: l, Reason: boom}) {
			t.Errorf("trapping watcher log: %#v", log)
		}
	}
}

// incrementer reads the counter, then sets it one higher: two of them
// racing can lose an update.
type incrementer struct{ counter proc.PID }

func (incrementer) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (incrementer) HandleCall(s struct{}, _ struct{}, _ genserver.From[struct{}]) (struct{}, []molecule.Effect) {
	return s, nil
}

func (i incrementer) HandleCast(s struct{}, _ goNow) (struct{}, []molecule.Effect) {
	return s, molecule.Do(molecule.SendRequest{To: i.counter, Req: get{}, Tag: "read"})
}

func (i incrementer) HandleInfo(s struct{}, msg any) (struct{}, []molecule.Effect) {
	if r, ok := msg.(molecule.Response); ok && r.Tag == "read" {
		return s, molecule.Do(molecule.SendRequest{To: i.counter, Req: set{r.Value.(int) + 1}, Tag: "write"})
	}
	return s, nil
}

func race(t *testing.T, seed uint64) int {
	s := gensim.New(seed)
	c := spawn(t, s, genserver.Gen(counter{}))
	for range 2 {
		s.Cast(spawn(t, s, genserver.Gen(incrementer{c})), goNow{})
	}
	s.RunUntilIdle()
	n, _ := gensim.State[int](s, c)
	return n
}

// TestFindsLostUpdate searches seeds for the interleaving losing an
// update, then replays it.
func TestFindsLostUpdate(t *testing.T) {
	for seed := range uint64(100) {
		if n := race(t, seed); n == 1 {
			if again := race(t, seed); again != 1 {
				t.Fatalf("seed %d lost an update, but not when replayed", seed)
			}
			return
		}
	}
	t.Error("no seed out of 100 lost an update")
}

func TestRequestTimeout(t *testing.T) {
	s := gensim.New(1)
	c := spawn(t, s, genserver.Gen(counter{}))
	r := spawn(t, s, genserver.Gen(requester{c}))
	log := func() []any {
		l, _ := gensim.State[[]any](s, r)
		return l
	}
	// A request to a server that never answers gives up after its
	// timeout, in virtual time.
	s.Cast(r, goNow{})
	s.Advance(time.Second - time.Nanosecond)
	if got := log(); len(got) != 1 {
		t.Fatalf("before the timeout: %#v", got)
	}
	s.Advance(time.Nanosecond)
	got := log()
	if len(got) != 2 || got[1].(molecule.Response).Err != context.DeadlineExceeded {
		t.Errorf("at the timeout: %#v", got)
	}
}

// requester sends a request that is never answered, with a timeout.
type requester struct{ to proc.PID }

func (requester) Init(proc.PID) ([]any, []molecule.Effect, error) { return nil, nil, nil }

func (requester) HandleCall(log []any, _ struct{}, _ genserver.From[struct{}]) ([]any, []molecule.Effect) {
	return log, nil
}

func (q requester) HandleCast(log []any, _ goNow) ([]any, []molecule.Effect) {
	return append(slices.Clip(log), "sent"), molecule.Do(molecule.SendRequest{To: q.to, Req: mute{}, Tag: "r", Timeout: time.Second})
}

func (requester) HandleInfo(log []any, msg any) ([]any, []molecule.Effect) {
	if r, ok := msg.(molecule.Response); ok {
		return append(slices.Clip(log), r), nil
	}
	return log, nil
}
