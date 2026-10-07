package proc

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRegister(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		a, _ := actor(n, nil)
		b, _ := actor(n, nil)

		if err := n.Register("a", a.pid); err != nil {
			t.Fatal(err)
		}
		if err := n.Register("a", a.pid); err != nil {
			t.Errorf("re-registering the same pair: %v", err)
		}
		if pid, ok := n.WhereIs("a"); !ok || pid != a.pid {
			t.Errorf("WhereIs(a) = %v, %v", pid, ok)
		}
		if err := n.Register("a", b.pid); err != ErrNameTaken {
			t.Errorf("taken name: %v", err)
		}
		if err := n.Register("other", a.pid); err != ErrHasName {
			t.Errorf("second name: %v", err)
		}
		if err := n.Register("b", b.pid); err != nil {
			t.Fatal(err)
		}
		if got := n.Registered(); !slices.Equal(got, []string{"a", "b"}) {
			t.Errorf("Registered() = %v", got)
		}

		if !n.Unregister("a") || n.Unregister("a") {
			t.Error("Unregister should succeed once")
		}
		if _, ok := n.WhereIs("a"); ok {
			t.Error("name still registered")
		}
		if err := n.Register("renamed", a.pid); err != nil {
			t.Errorf("registering after Unregister: %v", err)
		}
	})
}

func TestRegisterInvalid(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		dead := n.spawn(func(*Self) error { return nil })
		synctest.Wait()
		live, _ := actor(n, nil)
		remote := live.pid
		remote.node = "other@host"

		for _, tt := range []struct {
			name string
			pid  PID
			want error
		}{
			{"", live.pid, ErrEmptyName},
			{"x", dead.pid, NoProc},
			{"x", remote, ErrNotLocal},
			{"x", PID{}, ErrNotLocal},
		} {
			if err := n.Register(tt.name, tt.pid); err != tt.want {
				t.Errorf("Register(%q, %v) = %v, want %v", tt.name, tt.pid, err, tt.want)
			}
		}
		if len(n.Registered()) != 0 {
			t.Errorf("Registered() = %v", n.Registered())
		}
	})
}

// TestNameFreedBeforeDown checks that whoever learns of a death can reuse
// the name right away, as a supervisor restarting a child does.
func TestNameFreedBeforeDown(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		old, _ := actor(n, nil)
		if err := n.Register("server", old.pid); err != nil {
			t.Fatal(err)
		}
		ctx, stop := n.Watch(context.Background(), old.pid)
		defer stop()

		// Like a supervisor: on EXIT, take over the name at once.
		ready := make(chan struct{})
		errc := make(chan error, 1)
		n.spawn(func(s *Self) error {
			s.TrapExit(true)
			s.Link(old.pid)
			close(ready)
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if _, ok := msg.(ExitMsg); !ok {
				t.Errorf("got %#v, want ExitMsg", msg)
			}
			errc <- n.Register("server", s.PID())
			_, err = s.Receive(context.Background())
			return err
		})
		<-ready

		n.Send(old.pid, exitWith{errBoom})
		<-ctx.Done()
		if pid, _ := n.WhereIs("server"); pid == old.pid {
			t.Error("name of the dead process still registered after DOWN")
		}
		if err := <-errc; err != nil {
			t.Errorf("registering the name on EXIT: %v", err)
		}
	})
}

// onDown runs f synchronously, inside the death of p, at the point where
// its monitors are notified.
func onDown(p *process, f func(reason error)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.watchedBy(p.node.MakeRef(), watcher{cancel: f})
}

// TestNameFreedBeforeNotify checks, at the very moment links and monitors
// are notified, that the name of the dying process is already gone.
func TestNameFreedBeforeNotify(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		old, _ := actor(n, nil)
		if err := n.Register("server", old.pid); err != nil {
			t.Fatal(err)
		}
		// linked dies synchronously from the exit signal of old, so its
		// own monitors fire while old is notifying its links.
		linked, _ := actor(n, func(s *Self) { s.Link(old.pid) })

		var atLink, atMonitor bool
		onDown(linked, func(error) { _, atLink = n.WhereIs("server") })
		onDown(old, func(error) { _, atMonitor = n.WhereIs("server") })

		n.Send(old.pid, exitWith{errBoom})
		synctest.Wait()
		if reasonOf(linked) != errBoom {
			t.Fatalf("linked process: reason = %v", reasonOf(linked))
		}
		if atLink || atMonitor {
			t.Errorf("name still registered when notifying: links=%v monitors=%v", atLink, atMonitor)
		}
	})
}

// TestStaleDeathKeepsNewOwner replays an interleaving where Unregister runs
// while the owner is dying: it can no longer reach the process to clear
// its name, and the name is taken by another process before the dying one
// cleans up. That cleanup must not remove the new owner.
func TestStaleDeathKeepsNewOwner(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		a, _ := actor(n, nil)
		b, _ := actor(n, nil)
		if err := n.Register("x", a.pid); err != nil {
			t.Fatal(err)
		}

		n.procs.del(a.pid.id) // a is on its way out, as in die
		n.Unregister("x")
		if err := n.Register("x", b.pid); err != nil {
			t.Fatal(err)
		}
		a.die(errBoom)

		if pid, ok := n.WhereIs("x"); !ok || pid != b.pid {
			t.Errorf("WhereIs(x) = %v, %v; want %v", pid, ok, b.pid)
		}
	})
}

func TestRegisterSameNameRace(t *testing.T) {
	const contenders = 100
	n := NewNode("")
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range contenders {
		p, _ := actor(n, nil)
		wg.Go(func() {
			<-start
			if n.Register("x", p.pid) == nil {
				wins.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()
	if wins.Load() != 1 {
		t.Errorf("%d processes registered the same name", wins.Load())
	}
}

// TestRegisterDeathRace registers processes while they die. Once a death
// has been observed, the name must be gone whoever won the race.
func TestRegisterDeathRace(t *testing.T) {
	n := NewNode("")
	for i := range 1000 {
		p, _ := actor(n, nil)
		ctx, stop := n.Watch(context.Background(), p.pid)
		go n.Send(p.pid, exitWith{errBoom})
		err := n.Register("racer", p.pid)
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("process did not die")
		}
		stop()
		if pid, ok := n.WhereIs("racer"); ok {
			t.Fatalf("round %d: name left on dead %v (Register: %v)", i, pid, err)
		}
	}
}
