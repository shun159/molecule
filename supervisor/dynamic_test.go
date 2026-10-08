package supervisor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

func (w *world) startDynamic(spec supervisor.DynamicSpec) proc.PID {
	w.t.Helper()
	pid, err := supervisor.StartDynamic(context.Background(), w.n, spec)
	if err != nil {
		w.t.Fatal(err)
	}
	w.kill = append(w.kill, pid)
	return pid
}

func (w *world) startChild(sup proc.PID, spec supervisor.ChildSpec) proc.PID {
	w.t.Helper()
	pid, err := supervisor.StartChild(context.Background(), w.n, sup, spec)
	if err != nil {
		w.t.Fatal(err)
	}
	return pid
}

func (w *world) count(sup proc.PID) int {
	w.t.Helper()
	n, err := supervisor.CountChildren(context.Background(), w.n, sup)
	if err != nil {
		w.t.Fatal(err)
	}
	return n
}

func (w *world) crash(pid proc.PID, req any) {
	w.t.Helper()
	if _, err := molecule.Call(context.Background(), w.n, pid, req); err != nil {
		w.t.Fatal(err)
	}
}

func TestDynamicStartAndTerminate(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{})
		if w.count(sup) != 0 {
			t.Fatal("dynamic supervisor started with children")
		}
		a := w.startChild(sup, w.worker("a", supervisor.Temporary))
		b := w.startChild(sup, w.worker("b", supervisor.Temporary))
		w.expect(started{"a"}, started{"b"})

		infos, _ := supervisor.WhichChildren(context.Background(), w.n, sup)
		if len(infos) != 2 || infos[0].PID != a || infos[1].PID != b {
			t.Errorf("children = %+v", infos)
		}

		if err := supervisor.TerminateChild(context.Background(), w.n, sup, a); err != nil {
			t.Fatal(err)
		}
		w.expect(stopped{"a", proc.Shutdown})
		if w.count(sup) != 1 || w.n.IsAlive(a) {
			t.Errorf("after TerminateChild: count %d, alive %v", w.count(sup), w.n.IsAlive(a))
		}
		if err := supervisor.TerminateChild(context.Background(), w.n, sup, a); err != supervisor.ErrNotFound {
			t.Errorf("TerminateChild of a stopped child = %v", err)
		}
	})
}

// TestDynamicTerminatePermanent stops a permanent child: its EXIT, arriving
// afterwards, must not get it restarted.
func TestDynamicTerminatePermanent(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{})
		a := w.startChild(sup, w.worker("a", supervisor.Permanent))
		w.expect(started{"a"})
		if err := supervisor.TerminateChild(context.Background(), w.n, sup, a); err != nil {
			t.Fatal(err)
		}
		w.expect(stopped{"a", proc.Shutdown})
		if n := w.count(sup); n != 0 {
			t.Errorf("count = %d after terminating the only child", n)
		}
	})
}

func TestDynamicRestart(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{Intensity: 10})
		perm := w.startChild(sup, w.worker("perm", supervisor.Permanent))
		trans := w.startChild(sup, w.worker("trans", supervisor.Transient))
		trans2 := w.startChild(sup, w.worker("trans2", supervisor.Transient))
		temp := w.startChild(sup, w.worker("temp", supervisor.Temporary))
		w.expect(started{"perm"}, started{"trans"}, started{"trans2"}, started{"temp"})

		w.crash(perm, crash{})
		w.expect(stopped{"perm", errBoom}, started{"perm"})
		w.crash(trans, crash{})
		w.expect(stopped{"trans", errBoom}, started{"trans"})
		w.crash(trans2, stopNormal{})
		w.expect(stopped{"trans2", proc.Normal})
		w.crash(temp, crash{})
		w.expect(stopped{"temp", errBoom})

		infos, _ := supervisor.WhichChildren(context.Background(), w.n, sup)
		if len(infos) != 2 {
			t.Fatalf("children = %+v", infos)
		}
		for _, c := range infos {
			if c.PID == perm || c.PID == trans || !w.n.IsAlive(c.PID) {
				t.Errorf("child %+v: want a new, running process", c)
			}
		}
	})
}

func TestDynamicMaxChildren(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{MaxChildren: 2})
		a := w.startChild(sup, w.worker("a", supervisor.Temporary))
		w.startChild(sup, w.worker("b", supervisor.Temporary))
		w.expect(started{"a"}, started{"b"})

		if _, err := supervisor.StartChild(context.Background(), w.n, sup, w.worker("c", supervisor.Temporary)); err != supervisor.ErrMaxChildren {
			t.Errorf("third StartChild = %v", err)
		}
		w.expect()

		w.crash(a, stopNormal{})
		w.expect(stopped{"a", proc.Normal})
		w.startChild(sup, w.worker("c", supervisor.Temporary))
		w.expect(started{"c"})
	})
}

func TestDynamicStartFailures(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{})
		ignore := func(context.Context, *proc.Self) (proc.PID, error) { return proc.PID{}, molecule.ErrIgnore }
		if _, err := supervisor.StartChild(context.Background(), w.n, sup, supervisor.ChildSpec{Start: ignore}); err != molecule.ErrIgnore {
			t.Errorf("ignored child: %v", err)
		}
		var fail atomic.Int32
		fail.Store(1)
		failing := supervisor.ChildSpec{Start: genserver.StartLinkFunc(worker{id: "f", observer: w.observer, failInit: &fail})}
		if _, err := supervisor.StartChild(context.Background(), w.n, sup, failing); !errors.Is(err, errInit) {
			t.Errorf("failing child: %v", err)
		}
		if _, err := supervisor.StartChild(context.Background(), w.n, sup, supervisor.ChildSpec{}); err == nil {
			t.Error("child without Start accepted")
		}
		if w.count(sup) != 0 || !w.n.IsAlive(sup) {
			t.Errorf("count %d, supervisor alive %v", w.count(sup), w.n.IsAlive(sup))
		}
	})
}

// TestDynamicParallelShutdown stops children that each take their whole
// Shutdown: stopped at once, they take one Shutdown in all, not one each.
func TestDynamicParallelShutdown(t *testing.T) {
	inWorld(t, func(w *world) {
		sups := make(chan proc.PID, 1)
		parent := w.n.Spawn(func(s *proc.Self) error {
			sup, err := supervisor.StartDynamicLink(context.Background(), s, supervisor.DynamicSpec{})
			if err != nil {
				return err
			}
			sups <- sup
			_, err = s.Receive(context.Background())
			return err
		})
		sup := <-sups
		var children []proc.PID
		for range 3 {
			children = append(children, w.startChild(sup, supervisor.ChildSpec{Start: stubborn, Shutdown: time.Second}))
		}
		down, stop := w.n.Watch(context.Background(), sup)
		defer stop()

		start := time.Now()
		kill(w.n, parent, proc.Shutdown)
		<-down.Done()
		if c, d := context.Cause(down), time.Since(start); c != proc.Shutdown || d != time.Second {
			t.Errorf("supervisor: %v after %v; want shutdown after 1s", c, d)
		}
		for _, pid := range children {
			if w.n.IsAlive(pid) {
				t.Errorf("child %v survived", pid)
			}
		}
	})
}

func TestDynamicMaxIntensity(t *testing.T) {
	inWorld(t, func(w *world) {
		sup := w.startDynamic(supervisor.DynamicSpec{Intensity: 1})
		a := w.startChild(sup, w.worker("a", supervisor.Permanent))
		b := w.startChild(sup, w.worker("b", supervisor.Permanent))
		w.expect(started{"a"}, started{"b"})
		down, stop := w.n.Watch(context.Background(), sup)
		defer stop()

		w.crash(a, crash{})
		w.expect(stopped{"a", errBoom}, started{"a"})
		// A restarted child keeps its place among the children.
		infos, _ := supervisor.WhichChildren(context.Background(), w.n, sup)
		if len(infos) != 2 || infos[1].PID != b || infos[0].PID == a {
			t.Fatalf("children after restart = %+v", infos)
		}
		w.crash(infos[0].PID, crash{})
		<-down.Done()
		if c := context.Cause(down); c != supervisor.ErrMaxIntensity {
			t.Errorf("exit reason = %v", c)
		}
		w.expect(stopped{"a", errBoom}, stopped{"b", proc.Shutdown})
	})
}

func TestDynamicRestartRetry(t *testing.T) {
	inWorld(t, func(w *world) {
		var fail atomic.Int32
		sup := w.startDynamic(supervisor.DynamicSpec{Intensity: 5})
		pid := w.startChild(sup, supervisor.ChildSpec{
			Start: genserver.StartLinkFunc(worker{id: "a", observer: w.observer, failInit: &fail}),
		})
		w.expect(started{"a"})
		fail.Store(2)
		w.crash(pid, crash{})
		w.expect(stopped{"a", errBoom}, started{"a"})
		if w.count(sup) != 1 {
			t.Errorf("count = %d", w.count(sup))
		}
	})
}

func TestDynamicName(t *testing.T) {
	inWorld(t, func(w *world) {
		name := molecule.Local("conns")
		w.startDynamic(supervisor.DynamicSpec{Name: name})
		if _, err := supervisor.StartChild(context.Background(), w.n, name, w.worker("a", supervisor.Temporary)); err != nil {
			t.Errorf("StartChild by name: %v", err)
		}
		w.expect(started{"a"})
		var already *molecule.AlreadyStartedError
		if _, err := supervisor.StartDynamic(context.Background(), w.n, supervisor.DynamicSpec{Name: name}); !errors.As(err, &already) {
			t.Errorf("second StartDynamic = %v", err)
		}
	})
}

func TestStop(t *testing.T) {
	inWorld(t, func(w *world) {
		static := w.start(supervisor.Spec{Children: []supervisor.ChildSpec{
			w.worker("a", supervisor.Permanent),
			w.worker("b", supervisor.Permanent),
		}})
		w.expect(started{"a"}, started{"b"})
		if err := supervisor.Stop(context.Background(), w.n, static); err != nil {
			t.Fatal(err)
		}
		w.expect(stopped{"b", proc.Shutdown}, stopped{"a", proc.Shutdown})
		if w.n.IsAlive(static) {
			t.Error("static supervisor alive after Stop")
		}

		dynamic := w.startDynamic(supervisor.DynamicSpec{})
		w.startChild(dynamic, w.worker("c", supervisor.Permanent))
		w.expect(started{"c"})
		if err := supervisor.Stop(context.Background(), w.n, dynamic); err != nil {
			t.Fatal(err)
		}
		w.expect(stopped{"c", proc.Shutdown})
		if w.n.IsAlive(dynamic) {
			t.Error("dynamic supervisor alive after Stop")
		}
		if err := supervisor.Stop(context.Background(), w.n, dynamic); !errors.Is(err, proc.NoProc) {
			t.Errorf("Stop of a stopped supervisor = %v", err)
		}
	})
}
