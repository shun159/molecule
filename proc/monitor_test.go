package proc

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func monitorCounts(p *process) (monitors, monitoring int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.monitors), len(p.monitoring)
}

func TestMonitorDown(t *testing.T) {
	for _, reason := range []error{Normal, errBoom, Kill} {
		t.Run(reason.Error(), func(t *testing.T) {
			bubble(t, func(t *testing.T, n *Node) {
				target, _ := actor(n, nil)
				var ref Ref
				watcher, inbox := actor(n, func(s *Self) { ref = s.Monitor(target.pid) })

				n.Send(target.pid, exitWith{reason})
				synctest.Wait()
				if m := <-inbox; m != (DownMsg{Ref: ref, PID: target.pid, Reason: reason}) {
					t.Errorf("got %#v", m)
				}
				if !alive(watcher) {
					t.Errorf("watcher died with %v", reasonOf(watcher))
				}
				if _, monitoring := monitorCounts(watcher); monitoring != 0 {
					t.Error("watcher still tracks a fired monitor")
				}
			})
		})
	}
}

func TestMonitorKilled(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		_, inbox := actor(n, func(s *Self) { s.Monitor(target.pid) })
		do(n, func(s *Self) { s.Exit(target.pid, Kill) })
		synctest.Wait()
		if m := (<-inbox).(DownMsg); m.Reason != Killed {
			t.Errorf("reason = %v, want killed", m.Reason)
		}
	})
}

func TestMonitorNoProc(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		dead := n.spawn(func(*Self) error { return nil })
		synctest.Wait()
		remote := dead.pid
		remote.node = "other@host"

		var r1, r2 Ref
		p, inbox := actor(n, func(s *Self) {
			r1 = s.Monitor(dead.pid)
			r2 = s.Monitor(remote)
		})
		if m := <-inbox; m != (DownMsg{Ref: r1, PID: dead.pid, Reason: NoProc}) {
			t.Errorf("got %#v", m)
		}
		if m := <-inbox; m != (DownMsg{Ref: r2, PID: remote, Reason: NoConnection}) {
			t.Errorf("got %#v", m)
		}
		if _, monitoring := monitorCounts(p); monitoring != 0 {
			t.Error("monitor of a dead process was registered")
		}
	})
}

func TestMonitorMany(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		refs := make(map[Ref]bool)
		_, inbox := actor(n, func(s *Self) {
			for range 3 {
				refs[s.Monitor(target.pid)] = true
			}
		})
		n.Send(target.pid, exitWith{errBoom})
		synctest.Wait()
		for range 3 {
			m := (<-inbox).(DownMsg)
			if !refs[m.Ref] {
				t.Fatalf("unexpected or duplicate %#v", m)
			}
			delete(refs, m.Ref)
		}
	})
}

func TestMonitorSelf(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		p, _ := actor(n, func(s *Self) { s.Monitor(s.PID()) })
		if monitors, monitoring := monitorCounts(p); monitors+monitoring != 0 {
			t.Error("self monitor registered")
		}
	})
}

func TestDemonitor(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		var active bool
		watcher, inbox := actor(n, func(s *Self) {
			active = s.Demonitor(s.Monitor(target.pid))
		})
		if !active {
			t.Error("Demonitor of a live monitor returned false")
		}
		if monitors, _ := monitorCounts(target); monitors != 0 {
			t.Error("target still tracks a removed monitor")
		}
		n.Send(target.pid, exitWith{errBoom})
		synctest.Wait()
		select {
		case m := <-inbox:
			t.Errorf("got %#v after Demonitor", m)
		default:
		}
		if !alive(watcher) {
			t.Error("watcher died")
		}
	})
}

func TestDemonitorFlush(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		got := make(chan []any, 1)
		n.spawn(func(s *Self) error {
			ref := s.Monitor(target.pid)
			s.Send(target.pid, exitWith{errBoom})
			s.Send(s.PID(), "before")
			for s.p.mbox.len() < 2 { // wait for the DOWN behind "before"
				time.Sleep(time.Millisecond)
			}
			s.Send(s.PID(), "after")
			if s.Demonitor(ref) {
				t.Error("Demonitor of a fired monitor returned true")
			}
			var msgs []any
			for range 2 {
				m, _ := s.Receive(context.Background())
				msgs = append(msgs, m)
			}
			got <- msgs
			return nil
		})
		if msgs := <-got; len(msgs) != 2 || msgs[0] != "before" || msgs[1] != "after" {
			t.Errorf("mailbox after flush = %v", msgs)
		}
	})
}

func TestWatcherDeathCleansTarget(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		watcher, _ := actor(n, func(s *Self) { s.Monitor(target.pid) })
		if monitors, _ := monitorCounts(target); monitors != 1 {
			t.Fatalf("target monitors = %d, want 1", monitors)
		}
		n.Send(watcher.pid, exitWith{errBoom})
		synctest.Wait()
		if monitors, _ := monitorCounts(target); monitors != 0 {
			t.Error("target still tracks the monitor of a dead watcher")
		}
		if !alive(target) {
			t.Error("target died with its watcher")
		}
	})
}

// TestMonitorRace monitors a process that dies concurrently. Each watcher
// must get exactly one DOWN: with the real reason or NoProc.
func TestMonitorRace(t *testing.T) {
	const watchers = 200
	n := NewNode("")
	target, _ := actor(n, nil)

	start := make(chan struct{})
	inboxes := make([]chan DownMsg, watchers)
	for i := range watchers {
		inbox := make(chan DownMsg, 2)
		inboxes[i] = inbox
		n.spawn(func(s *Self) error {
			<-start
			ref := s.Monitor(target.pid)
			for {
				msg, err := s.Receive(context.Background())
				if err != nil {
					return err
				}
				d := msg.(DownMsg)
				if d.Ref != ref {
					t.Errorf("DOWN for unknown ref %v", d.Ref)
				}
				inbox <- d
			}
		})
	}
	close(start)
	n.Send(target.pid, exitWith{errBoom})

	timeout := time.After(5 * time.Second)
	for i, inbox := range inboxes {
		select {
		case m := <-inbox:
			if m.PID != target.pid || (m.Reason != errBoom && m.Reason != NoProc) {
				t.Fatalf("watcher %d: got %#v", i, m)
			}
		case <-timeout:
			t.Fatalf("watcher %d never got DOWN", i)
		}
	}
	for i, inbox := range inboxes {
		select {
		case m := <-inbox:
			t.Fatalf("watcher %d: duplicate %#v", i, m)
		default:
		}
	}
}

// TestDemonitorRace demonitors while the target dies. After Demonitor
// returns, no DOWN for that ref may be in the mailbox or arrive later.
func TestDemonitorRace(t *testing.T) {
	for range 300 {
		bubble(t, func(t *testing.T, n *Node) {
			target, _ := actor(n, nil)
			hold := make(chan struct{})
			watcher := n.spawn(func(s *Self) error {
				ref := s.Monitor(target.pid)
				s.Send(target.pid, exitWith{errBoom})
				s.Demonitor(ref)
				<-hold
				return nil
			})
			synctest.Wait() // the target is dead and every DOWN delivered
			if msg, ok := watcher.mbox.pop(); ok {
				t.Errorf("message after Demonitor: %#v", msg)
			}
			close(hold)
		})
		if t.Failed() {
			return
		}
	}
}

func TestMailboxRemove(t *testing.T) {
	m := newMailbox()
	for i := range 6 {
		m.push(i)
	}
	m.pop()
	m.remove(func(v any) bool { return v.(int)%2 == 0 })
	var got []int
	for v, ok := m.pop(); ok; v, ok = m.pop() {
		got = append(got, v.(int))
	}
	if len(got) != 3 || got[0] != 1 || got[1] != 3 || got[2] != 5 {
		t.Errorf("got %v", got)
	}
}
