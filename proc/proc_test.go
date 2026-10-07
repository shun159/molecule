package proc

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// exitReason waits for p to die and returns its exit reason.
func exitReason(t *testing.T, p *process) error {
	t.Helper()
	select {
	case <-p.done:
		return p.exitReason()
	case <-time.After(5 * time.Second):
		t.Fatalf("%v did not exit", p.pid)
		return nil
	}
}

func TestExitReason(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name  string
		fn    func(*Self) error
		check func(error) bool
	}{
		{"nil is normal", func(*Self) error { return nil }, func(r error) bool { return r == Normal }},
		{"error is kept", func(*Self) error { return boom }, func(r error) bool { return r == boom }},
		{"wrapped shutdown", func(*Self) error { return fmt.Errorf("%w: bye", Shutdown) },
			func(r error) bool { return errors.Is(r, Shutdown) }},
		{"goexit", func(*Self) error { runtime.Goexit(); return nil }, func(r error) bool { return r == ErrGoexit }},
		{"panic", func(*Self) error { panic("oops") }, func(r error) bool {
			var pe *PanicError
			return errors.As(r, &pe) && pe.Value == "oops" && len(pe.Stack) > 0
		}},
		{"panic with error", func(*Self) error { panic(boom) }, func(r error) bool { return errors.Is(r, boom) }},
		{"panic nil", func(*Self) error { panic(nil) }, func(r error) bool {
			var pe *PanicError
			return errors.As(r, &pe)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := NewNode("")
			if r := exitReason(t, n.spawn(tt.fn)); !tt.check(r) {
				t.Errorf("unexpected exit reason %v", r)
			}
		})
	}
}

func TestDeadProcess(t *testing.T) {
	n := NewNode("")
	p := n.spawn(func(*Self) error { return nil })
	exitReason(t, p)

	if n.IsAlive(p.pid) {
		t.Error("dead process reported alive")
	}
	if n.procs.get(p.pid.id) != nil {
		t.Error("dead process left in the process table")
	}
	n.Send(p.pid, "dropped") // must not panic or block
}

func TestPID(t *testing.T) {
	n := NewNode("a@host")
	p := n.spawn(func(s *Self) error {
		_, err := s.Receive(context.Background())
		return err
	})
	defer n.Send(p.pid, "stop")

	if !n.IsAlive(p.pid) {
		t.Fatal("process not alive")
	}
	if p.pid.Node() != "a@host" || p.pid.IsZero() {
		t.Errorf("bad pid %v", p.pid)
	}
	if !(PID{}).IsZero() {
		t.Error("zero PID is not zero")
	}

	stale := p.pid
	stale.creation++
	if n.IsAlive(stale) {
		t.Error("pid of another creation reported alive")
	}
	remote := p.pid
	remote.node = "b@host"
	if n.IsAlive(remote) {
		t.Error("remote pid reported alive")
	}
	n.Send(stale, "dropped")
	n.Send(remote, "dropped")

	if got := n.NewPIDForTest(p.pid.id); got != p.pid {
		t.Errorf("PID equality depends on more than its data: %v != %v", got, p.pid)
	}
}

// NewPIDForTest rebuilds a local PID from its parts, as decoding one from
// the wire would.
func (n *Node) NewPIDForTest(id uint64) PID {
	return PID{node: n.name, creation: n.creation, id: id}
}

func TestEcho(t *testing.T) {
	n := NewNode("")
	type ping struct {
		from PID
		n    int
	}
	echo := n.Spawn(func(s *Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			switch m := msg.(type) {
			case ping:
				s.Send(m.from, m.n)
			case string:
				return nil
			}
		}
	})

	got := make(chan []any, 1)
	n.Spawn(func(s *Self) error {
		var replies []any
		for i := range 3 {
			s.Send(echo, ping{s.PID(), i})
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			replies = append(replies, msg)
		}
		s.Send(echo, "stop")
		got <- replies
		return nil
	})

	if r := <-got; fmt.Sprint(r) != "[0 1 2]" {
		t.Errorf("replies = %v", r)
	}
}

func TestMailboxOrder(t *testing.T) {
	const senders, perSender = 8, 5000
	n := NewNode("")
	type msg struct{ from, seq int }

	errc := make(chan error, 1)
	recv := n.Spawn(func(s *Self) error {
		next := make([]int, senders)
		for range senders * perSender {
			m, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			mm := m.(msg)
			if mm.seq != next[mm.from] {
				errc <- fmt.Errorf("sender %d: got seq %d, want %d", mm.from, mm.seq, next[mm.from])
				return nil
			}
			next[mm.from]++
		}
		errc <- nil
		return nil
	})

	var wg sync.WaitGroup
	for i := range senders {
		wg.Go(func() {
			for seq := range perSender {
				n.Send(recv, msg{i, seq})
			}
		})
	}
	wg.Wait()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestMailboxCompaction(t *testing.T) {
	m := newMailbox()
	for i := range 3 * compactThreshold {
		m.push(i)
	}
	for i := range 2 * compactThreshold {
		if v, _ := m.pop(); v != i {
			t.Fatalf("pop = %v, want %d", v, i)
		}
	}
	if m.head >= compactThreshold {
		t.Errorf("queue not compacted: head = %d", m.head)
	}
	for i := 2 * compactThreshold; i < 3*compactThreshold; i++ {
		if v, _ := m.pop(); v != i {
			t.Fatalf("pop = %v, want %d", v, i)
		}
	}
	if _, ok := m.pop(); ok || m.len() != 0 {
		t.Error("mailbox not empty")
	}
}

func TestReceiveTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := NewNode("")
		start := time.Now()
		p := n.spawn(func(s *Self) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := s.Receive(ctx)
			return err
		})
		<-p.done
		if r := p.exitReason(); !errors.Is(r, context.DeadlineExceeded) {
			t.Errorf("exit reason = %v", r)
		}
		if d := time.Since(start); d != time.Second {
			t.Errorf("timed out after %v", d)
		}
	})
}

func TestReceiveWakesOnSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := NewNode("")
		got := make(chan any, 1)
		pid := n.Spawn(func(s *Self) error {
			msg, err := s.Receive(context.Background())
			got <- msg
			return err
		})
		synctest.Wait() // the process is now blocked in Receive
		n.Send(pid, "hello")
		if msg := <-got; msg != "hello" {
			t.Errorf("got %v", msg)
		}
	})
}

func TestSelfContextCause(t *testing.T) {
	n := NewNode("")
	boom := errors.New("boom")
	ctxc := make(chan context.Context, 1)
	p := n.spawn(func(s *Self) error {
		ctxc <- s.Context()
		return boom
	})
	exitReason(t, p)
	if c := context.Cause(<-ctxc); c != boom {
		t.Errorf("context cause = %v", c)
	}
}
