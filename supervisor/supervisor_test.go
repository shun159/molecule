package supervisor_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/internal/testlog"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

var (
	errBoom = errors.New("boom")
	errInit = errors.New("init failed")
)

// Events reported by workers to the observer.
type (
	started struct{ id string }
	stopped struct {
		id     string
		reason error
	}
)

// worker reports its start and stop to an observer. It traps exits so that
// it stops through Terminate. failInit, if set, makes Init fail while it is
// positive, counting down.
type worker struct {
	id       string
	observer proc.PID
	failInit *atomic.Int32
}

type (
	crash      struct{}
	stopNormal struct{}
)

func (w worker) Init(proc.PID) (worker, []gen.Effect, error) {
	if w.failInit != nil && w.failInit.Add(-1) >= 0 {
		return w, nil, errInit
	}
	return w, gen.Do(gen.TrapExit{On: true}, gen.Send{To: w.observer, Msg: started{w.id}}), nil
}

func (worker) HandleCall(w worker, req any, from genserver.From[string]) (worker, []gen.Effect) {
	switch req.(type) {
	case crash:
		return w, gen.Do(from.Reply("ok"), gen.Stop{Reason: errBoom})
	case stopNormal:
		return w, gen.Do(from.Reply("ok"), gen.Stop{})
	}
	return w, gen.Do(from.Reply("ok"))
}

func (worker) HandleCast(w worker, _ struct{}) (worker, []gen.Effect) { return w, nil }

func (worker) Terminate(w worker, reason error) []gen.Effect {
	return gen.Do(gen.Send{To: w.observer, Msg: stopped{w.id, reason}})
}

// world is a synctest bubble with a node and an observer collecting the
// worker events.
type world struct {
	t        *testing.T
	n        *proc.Node
	log      *testlog.Recorder
	observer proc.PID
	events   chan any
	kill     []proc.PID
}

func inWorld(t *testing.T, f func(w *world)) {
	synctest.Test(t, func(t *testing.T) {
		rec, logger := testlog.New()
		w := &world{t: t, n: proc.NewNode("", proc.WithLogger(logger)), log: rec, events: make(chan any, 64)}
		w.observer = w.n.Spawn(func(s *proc.Self) error {
			for {
				msg, err := s.Receive(context.Background())
				if err != nil {
					return err
				}
				w.events <- msg
			}
		})
		f(w)
		for _, pid := range append(w.kill, w.observer) {
			kill(w.n, pid, proc.Kill)
		}
	})
}

func kill(n *proc.Node, pid proc.PID, reason error) {
	ctx, stop := n.Watch(context.Background(), pid)
	defer stop()
	n.Spawn(func(s *proc.Self) error { s.Exit(pid, reason); return nil })
	<-ctx.Done()
}

func (w *world) worker(id string, restart supervisor.Restart) supervisor.ChildSpec {
	return supervisor.ChildSpec{
		ID:      id,
		Start:   genserver.StartLinkFunc(worker{id: id, observer: w.observer}),
		Restart: restart,
	}
}

func (w *world) start(spec supervisor.Spec) proc.PID {
	w.t.Helper()
	pid, err := supervisor.Start(context.Background(), w.n, spec)
	if err != nil {
		w.t.Fatal(err)
	}
	w.kill = append(w.kill, pid)
	return pid
}

// expect checks the next events, in order, and that no other follows.
func (w *world) expect(want ...any) {
	w.t.Helper()
	synctest.Wait()
	var got []any
	for len(got) < len(want) {
		select {
		case e := <-w.events:
			got = append(got, e)
		default:
			w.t.Fatalf("events = %#v\nwant %#v", got, want)
		}
	}
	synctest.Wait()
	select {
	case e := <-w.events:
		got = append(got, e)
	default:
	}
	if !reflect.DeepEqual(got, want) {
		w.t.Fatalf("events = %#v\nwant %#v", got, want)
	}
}

func (w *world) children(sup gen.Dest) map[string]proc.PID {
	w.t.Helper()
	infos, err := supervisor.WhichChildren(context.Background(), w.n, sup)
	if err != nil {
		w.t.Fatal(err)
	}
	m := make(map[string]proc.PID)
	for _, c := range infos {
		m[c.ID] = c.PID
	}
	return m
}

func (w *world) call(sup proc.PID, id string, req any) {
	w.t.Helper()
	pid := w.children(sup)[id]
	if _, err := gen.Call(context.Background(), w.n, pid, req); err != nil {
		w.t.Fatal(err)
	}
}

func TestStartOrder(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.start(supervisor.Spec{Children: []supervisor.ChildSpec{
			w.worker("a", supervisor.Permanent),
			w.worker("b", supervisor.Permanent),
			w.worker("c", supervisor.Permanent),
		}})
		w.expect(started{"a"}, started{"b"}, started{"c"})

		infos, err := supervisor.WhichChildren(context.Background(), w.n, sup)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, c := range infos {
			ids = append(ids, c.ID)
			if !w.n.IsAlive(c.PID) {
				t.Errorf("child %s not running", c.ID)
			}
		}
		if !reflect.DeepEqual(ids, []string{"a", "b", "c"}) {
			t.Errorf("children = %v", ids)
		}
	})
}

func TestStrategies(t *testing.T) {
	for _, tt := range []struct {
		strategy supervisor.Strategy
		events   []any
		same     []string // children keeping their process
	}{
		{supervisor.OneForOne, []any{
			stopped{"b", errBoom}, started{"b"},
		}, []string{"a", "c"}},
		{supervisor.OneForAll, []any{
			stopped{"b", errBoom},
			stopped{"c", proc.Shutdown}, stopped{"a", proc.Shutdown},
			started{"a"}, started{"b"}, started{"c"},
		}, nil},
		{supervisor.RestForOne, []any{
			stopped{"b", errBoom},
			stopped{"c", proc.Shutdown},
			started{"b"}, started{"c"},
		}, []string{"a"}},
	} {
		t.Run(tt.strategy.String(), func(t *testing.T) {
			inWorld(t, func(w *world) {
				sup := w.start(supervisor.Spec{
					Strategy: tt.strategy,
					Children: []supervisor.ChildSpec{
						w.worker("a", supervisor.Permanent),
						w.worker("b", supervisor.Permanent),
						w.worker("c", supervisor.Permanent),
					},
				})
				w.expect(started{"a"}, started{"b"}, started{"c"})
				before := w.children(sup)

				w.call(sup, "b", crash{})
				w.expect(tt.events...)

				after := w.children(sup)
				for id, pid := range after {
					same := pid == before[id]
					want := false
					for _, s := range tt.same {
						want = want || s == id
					}
					if same != want || !w.n.IsAlive(pid) {
						t.Errorf("child %s: kept its process = %v, want %v (alive %v)", id, same, want, w.n.IsAlive(pid))
					}
				}
			})
		})
	}
}

func TestRestartTypes(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.start(supervisor.Spec{
			Intensity: 10,
			Children: []supervisor.ChildSpec{
				w.worker("permanent", supervisor.Permanent),
				w.worker("transient", supervisor.Transient),
				w.worker("transient2", supervisor.Transient),
				w.worker("temporary", supervisor.Temporary),
			},
		})
		w.expect(started{"permanent"}, started{"transient"}, started{"transient2"}, started{"temporary"})

		w.call(sup, "permanent", stopNormal{})
		w.expect(stopped{"permanent", proc.Normal}, started{"permanent"})

		w.call(sup, "transient", stopNormal{})
		w.expect(stopped{"transient", proc.Normal})

		w.call(sup, "transient2", crash{})
		w.expect(stopped{"transient2", errBoom}, started{"transient2"})

		w.call(sup, "temporary", crash{})
		w.expect(stopped{"temporary", errBoom})

		got := w.children(sup)
		if _, ok := got["temporary"]; ok {
			t.Error("temporary child still listed")
		}
		if pid, ok := got["transient"]; !ok || !pid.IsZero() {
			t.Errorf("transient child after a normal exit: %v, %v", pid, ok)
		}
	})
}

func TestMaxIntensity(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.start(supervisor.Spec{
			Intensity: 2,
			Period:    5 * time.Second,
			Children: []supervisor.ChildSpec{
				w.worker("a", supervisor.Permanent),
				w.worker("b", supervisor.Permanent),
			},
		})
		w.expect(started{"a"}, started{"b"})

		// Restarts spread out stay within the limit.
		for range 3 {
			w.call(sup, "b", crash{})
			w.expect(stopped{"b", errBoom}, started{"b"})
			time.Sleep(3 * time.Second)
		}
		time.Sleep(10 * time.Second) // let the window empty

		down, stop := w.n.Watch(context.Background(), sup)
		defer stop()
		w.call(sup, "b", crash{})
		w.expect(stopped{"b", errBoom}, started{"b"})
		w.call(sup, "b", crash{})
		w.expect(stopped{"b", errBoom}, started{"b"})
		if down.Err() != nil {
			t.Fatal("supervisor gave up at the limit")
		}
		w.call(sup, "b", crash{})
		w.expect(stopped{"b", errBoom}, stopped{"a", proc.Shutdown})
		<-down.Done()
		if c := context.Cause(down); c != supervisor.ErrMaxIntensity || !errors.Is(c, proc.Shutdown) {
			t.Errorf("exit reason = %v", c)
		}
	})
}

func TestStartFailure(t *testing.T) {
	inWorld(t, func(w *world) {
		var fail atomic.Int32
		fail.Store(1)
		_, err := supervisor.Start(context.Background(), w.n, supervisor.Spec{Children: []supervisor.ChildSpec{
			w.worker("a", supervisor.Permanent),
			{ID: "b", Start: genserver.StartLinkFunc(worker{id: "b", observer: w.observer, failInit: &fail})},
			w.worker("c", supervisor.Permanent),
		}})
		var se *supervisor.StartError
		if !errors.As(err, &se) || se.ID != "b" || !errors.Is(err, errInit) || !errors.Is(err, proc.Shutdown) {
			t.Errorf("Start = %v", err)
		}
		w.expect(started{"a"}, stopped{"a", proc.Shutdown})
	})
}

func TestIgnoredChild(t *testing.T) {
	inWorld(t, func(w *world) {
		ignore := func(context.Context, *proc.Self) (proc.PID, error) { return proc.PID{}, gen.ErrIgnore }
		sup := w.start(supervisor.Spec{Children: []supervisor.ChildSpec{
			{ID: "ignored", Start: ignore},
			w.worker("a", supervisor.Permanent),
		}})
		w.expect(started{"a"})
		if pid, ok := w.children(sup)["ignored"]; !ok || !pid.IsZero() {
			t.Errorf("ignored child: %v, %v", pid, ok)
		}
	})
}

// stubborn starts a child that traps exits and ignores them, so it only
// stops when killed.
func stubborn(ctx context.Context, parent *proc.Self) (proc.PID, error) {
	return parent.StartLink(ctx, func(s *proc.Self) error {
		s.TrapExit(true)
		s.InitAck(nil)
		for {
			if _, err := s.Receive(context.Background()); err != nil {
				return err
			}
		}
	})
}

func TestShutdown(t *testing.T) {
	for _, tt := range []struct {
		name     string
		shutdown time.Duration
		after    time.Duration
	}{
		{"timeout", time.Second, time.Second},
		{"brutal", supervisor.Brutal, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inWorld(t, func(w *world) {
				sup := w.start(supervisor.Spec{
					Strategy: supervisor.OneForAll,
					Children: []supervisor.ChildSpec{
						{ID: "stubborn", Start: stubborn, Shutdown: tt.shutdown},
						w.worker("b", supervisor.Permanent),
					},
				})
				w.expect(started{"b"})
				old := w.children(sup)["stubborn"]
				down, stop := w.n.Watch(context.Background(), old)
				defer stop()

				start := time.Now()
				w.call(sup, "b", crash{})
				<-down.Done()
				if d := time.Since(start); d != tt.after || context.Cause(down) != proc.Killed {
					t.Errorf("stubborn child: %v after %v", context.Cause(down), d)
				}
				w.expect(stopped{"b", errBoom}, started{"b"})
				w.kill = append(w.kill, w.children(sup)["stubborn"])
			})
		})
	}
}

// TestShutdownWaitsForSupervisors restarts an inner supervisor along with
// a sibling. The outer one must wait for the inner one to have stopped its
// own child before restarting anything.
func TestShutdownWaitsForSupervisors(t *testing.T) {
	inWorld(t, func(w *world) {
		inner := supervisor.Spec{Children: []supervisor.ChildSpec{w.worker("x", supervisor.Permanent)}}
		outer := w.start(supervisor.Spec{
			Strategy: supervisor.OneForAll,
			Children: []supervisor.ChildSpec{
				{ID: "inner", Start: supervisor.StartLinkFunc(inner), Type: supervisor.Supervisor},
				w.worker("w", supervisor.Permanent),
			},
		})
		w.expect(started{"x"}, started{"w"})

		w.call(outer, "w", crash{})
		w.expect(
			stopped{"w", errBoom},
			stopped{"x", proc.Shutdown},
			started{"x"}, started{"w"},
		)
	})
}

// TestSupervisorChildWaitsForever shuts down an inner supervisor that takes
// longer than DefaultShutdown to stop its own child. Being a supervisor,
// it is given all the time it needs rather than being killed.
func TestSupervisorChildWaitsForever(t *testing.T) {
	inWorld(t, func(w *world) {
		inner := supervisor.Spec{Children: []supervisor.ChildSpec{
			{ID: "slow", Start: stubborn, Shutdown: 10 * time.Second},
		}}
		outer := w.start(supervisor.Spec{
			Strategy: supervisor.OneForAll,
			Children: []supervisor.ChildSpec{
				{ID: "inner", Start: supervisor.StartLinkFunc(inner), Type: supervisor.Supervisor},
				w.worker("w", supervisor.Permanent),
			},
		})
		w.expect(started{"w"})
		old := w.children(outer)["inner"]
		down, stop := w.n.Watch(context.Background(), old)
		defer stop()

		start := time.Now()
		w.call(outer, "w", crash{})
		<-down.Done()
		if c, d := context.Cause(down), time.Since(start); c != proc.Shutdown || d != 10*time.Second {
			t.Errorf("inner supervisor: %v after %v; want shutdown after 10s", c, d)
		}
		w.expect(stopped{"w", errBoom}, started{"w"})
		w.kill = append(w.kill, w.children(w.children(outer)["inner"])["slow"])
	})
}

func TestParentExit(t *testing.T) {
	inWorld(t, func(w *world) {
		sups := make(chan proc.PID, 1)
		parent := w.n.Spawn(func(s *proc.Self) error {
			sup, err := supervisor.StartLink(context.Background(), s, supervisor.Spec{Children: []supervisor.ChildSpec{
				w.worker("a", supervisor.Permanent),
				w.worker("b", supervisor.Permanent),
			}})
			if err != nil {
				return err
			}
			sups <- sup
			_, err = s.Receive(context.Background())
			return err
		})
		sup := <-sups
		w.expect(started{"a"}, started{"b"})
		down, stop := w.n.Watch(context.Background(), sup)
		defer stop()

		kill(w.n, parent, proc.Shutdown)
		w.expect(stopped{"b", proc.Shutdown}, stopped{"a", proc.Shutdown})
		<-down.Done()
		if c := context.Cause(down); c != proc.Shutdown {
			t.Errorf("supervisor exit reason = %v", c)
		}
	})
}

func TestNestedRestart(t *testing.T) {
	inWorld(t, func(w *world) {
		inner := supervisor.Spec{Intensity: 1, Children: []supervisor.ChildSpec{w.worker("x", supervisor.Permanent)}}
		outer := w.start(supervisor.Spec{Intensity: 1, Children: []supervisor.ChildSpec{
			{ID: "inner", Start: supervisor.StartLinkFunc(inner), Type: supervisor.Supervisor},
		}})
		w.expect(started{"x"})
		innerPID := w.children(outer)["inner"]

		w.call(innerPID, "x", crash{})
		w.expect(stopped{"x", errBoom}, started{"x"})
		// Too many restarts for inner: it gives up and outer restarts it.
		w.call(innerPID, "x", crash{})
		w.expect(stopped{"x", errBoom}, started{"x"})

		if pid := w.children(outer)["inner"]; pid == innerPID || !w.n.IsAlive(pid) {
			t.Errorf("inner supervisor not restarted: %v", pid)
		}
	})
}

func TestRestartRetry(t *testing.T) {
	for _, tt := range []struct {
		name      string
		intensity int
		gaveUp    bool
	}{
		{"recovers", 5, false},
		{"gives up", 2, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			inWorld(t, func(w *world) {
				var fail atomic.Int32
				sup := w.start(supervisor.Spec{Intensity: tt.intensity, Children: []supervisor.ChildSpec{
					{ID: "a", Start: genserver.StartLinkFunc(worker{id: "a", observer: w.observer, failInit: &fail})},
				}})
				w.expect(started{"a"})
				down, stop := w.n.Watch(context.Background(), sup)
				defer stop()

				fail.Store(2) // the next two starts fail
				w.call(sup, "a", crash{})
				if tt.gaveUp {
					w.expect(stopped{"a", errBoom})
					<-down.Done()
					if c := context.Cause(down); c != supervisor.ErrMaxIntensity {
						t.Errorf("exit reason = %v", c)
					}
					return
				}
				w.expect(stopped{"a", errBoom}, started{"a"})
				if !w.n.IsAlive(w.children(sup)["a"]) {
					t.Error("child not running after the retries")
				}
			})
		})
	}
}

func TestSpecErrors(t *testing.T) {
	inWorld(t, func(w *world) {
		for _, spec := range []supervisor.Spec{
			{Children: []supervisor.ChildSpec{w.worker("a", supervisor.Permanent), w.worker("a", supervisor.Permanent)}},
			{Children: []supervisor.ChildSpec{{ID: "nostart"}}},
			{Children: []supervisor.ChildSpec{{Start: stubborn}}},
			{Strategy: 42},
		} {
			if _, err := supervisor.Start(context.Background(), w.n, spec); err == nil {
				t.Errorf("Start(%+v) succeeded", spec)
			}
		}
		w.expect()
	})
}

func TestName(t *testing.T) {
	inWorld(t, func(w *world) {
		name := gen.Local("sup")
		sup := w.start(supervisor.Spec{Name: name})
		if pid, ok := name.WhereIs(w.n); !ok || pid != sup {
			t.Errorf("WhereIs = %v, %v", pid, ok)
		}
		if _, err := supervisor.WhichChildren(context.Background(), w.n, name); err != nil {
			t.Errorf("WhichChildren by name: %v", err)
		}
		_, err := supervisor.Start(context.Background(), w.n, supervisor.Spec{Name: name})
		var already *gen.AlreadyStartedError
		if !errors.As(err, &already) || already.PID != sup {
			t.Errorf("second Start = %v", err)
		}
	})
}
