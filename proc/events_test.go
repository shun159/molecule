package proc

import (
	"sync"
	"testing"
	"testing/synctest"

	"github.com/shun159/molecule/internal/testlog"
)

// events collects the events of a node.
type events struct {
	mu sync.Mutex
	es []any
}

func (e *events) add(ev any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.es = append(e.es, ev)
}

func (e *events) all() []any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]any(nil), e.es...)
}

// A process failing is an event; one ending normally, killed or ended by
// an exit signal is none.
func TestEventCrashed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var evs events
		_, logger := testlog.New()
		n := NewNode("", WithEvents(evs.add), WithLogger(logger))
		failing := n.spawn(func(s *Self) error {
			s.SetLabel("failing")
			n.Register("f", s.PID())
			return errBoom
		})
		n.spawn(func(*Self) error { return nil })
		killed, _ := actor(n, nil)
		do(n, func(s *Self) { s.Exit(killed.pid, Kill) })
		signalled, _ := actor(n, nil)
		do(n, func(s *Self) { s.Link(signalled.pid); s.Exit(s.PID(), errBoom) })
		synctest.Wait()
		got := evs.all()
		want := Crashed{PID: failing.pid, Name: "f", Label: "failing", Reason: errBoom}
		if len(got) != 1 || got[0] != want {
			t.Errorf("events %v, want [%v]", got, want)
		}
	})
}

// A process killed for its mailbox is an event.
func TestEventOverflowed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var evs events
		_, logger := testlog.New()
		n := NewNode("", WithEvents(evs.add), WithLogger(logger))
		release := make(chan struct{})
		p := n.spawn(func(s *Self) error {
			s.SetLabel("slow")
			s.SetMaxMessages(1)
			<-release
			return nil
		})
		synctest.Wait()
		n.Send(p.pid, 0)
		n.Send(p.pid, 1)
		synctest.Wait()
		want := Overflowed{PID: p.pid, Label: "slow", Len: 2}
		if got := evs.all(); len(got) != 1 || got[0] != want {
			t.Errorf("events %v, want [%v]", got, want)
		}
		close(release)
	})
}

// A handler panicking is logged, and ends nothing.
func TestEventHandlerPanics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec, logger := testlog.New()
		n := NewNode("", WithEvents(func(any) { panic("bad handler") }), WithLogger(logger))
		p := n.spawn(func(*Self) error { return errBoom })
		synctest.Wait()
		if reasonOf(p) != errBoom {
			t.Errorf("reason %v", reasonOf(p))
		}
		if len(rec.Records("events: handler panicked")) != 1 {
			t.Error("panic not logged")
		}
		n.Emit("without a crash")
	})
}
