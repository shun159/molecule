package proc

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

var errBoom = errors.New("boom")

// exitWith makes an actor return the given reason.
type exitWith struct{ reason error }

// actor spawns a process that runs setup, then forwards everything it
// receives to the returned channel until it gets exitWith or dies.
// It returns once setup has completed.
func actor(n *Node, setup func(*Self)) (*process, <-chan any) {
	inbox := make(chan any, 64)
	ready := make(chan struct{})
	p := n.spawn(func(s *Self) error {
		if setup != nil {
			setup(s)
		}
		close(ready)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if m, ok := msg.(exitWith); ok {
				return m.reason
			}
			inbox <- msg
		}
	})
	<-ready
	return p, inbox
}

// do runs fn in a short-lived process and waits for it to finish.
func do(n *Node, fn func(*Self)) {
	<-n.spawn(func(s *Self) error { fn(s); return nil }).done
}

// bubble runs f in a synctest bubble and kills every process left on the
// node afterwards, so the bubble can end.
func bubble(t *testing.T, f func(t *testing.T, n *Node)) {
	synctest.Test(t, func(t *testing.T) {
		n := NewNode("")
		f(t, n)
		for _, p := range n.procs.all() {
			p.die(Killed)
		}
	})
}

func alive(p *process) bool { return !p.dead.Load() }

func reasonOf(p *process) error { return p.exitReason() }

func linkCount(p *process) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.links)
}

func TestExitSignalMatrix(t *testing.T) {
	type outcome struct {
		dies error // non-nil: target must die with this reason
		msg  error // non-nil: target must receive ExitMsg with this reason
	}
	tests := []struct {
		name    string
		viaLink bool
		reason  error
		noTrap  outcome
		trap    outcome
	}{
		{"link normal", true, Normal, outcome{}, outcome{msg: Normal}},
		{"link abnormal", true, errBoom, outcome{dies: errBoom}, outcome{msg: errBoom}},
		{"link kill is trappable", true, Kill, outcome{dies: Kill}, outcome{msg: Kill}},
		{"link killed", true, Killed, outcome{dies: Killed}, outcome{msg: Killed}},
		{"exit normal", false, Normal, outcome{}, outcome{msg: Normal}},
		{"exit abnormal", false, errBoom, outcome{dies: errBoom}, outcome{msg: errBoom}},
		{"exit kill", false, Kill, outcome{dies: Killed}, outcome{dies: Killed}},
	}
	for _, tt := range tests {
		for _, trap := range []bool{false, true} {
			name := tt.name + "/notrap"
			want := tt.noTrap
			if trap {
				name, want = tt.name+"/trap", tt.trap
			}
			t.Run(name, func(t *testing.T) {
				bubble(t, func(t *testing.T, n *Node) {
					source, _ := actor(n, nil)
					target, inbox := actor(n, func(s *Self) {
						s.TrapExit(trap)
						if tt.viaLink {
							s.Link(source.pid)
						}
					})

					if tt.viaLink {
						n.Send(source.pid, exitWith{tt.reason})
					} else {
						do(n, func(s *Self) { s.Exit(target.pid, tt.reason) })
					}
					synctest.Wait()

					from := source.pid
					if !tt.viaLink {
						from = PID{} // checked below only for links
					}
					switch {
					case want.dies != nil:
						if alive(target) || reasonOf(target) != want.dies {
							t.Errorf("target: alive=%v reason=%v, want death by %v",
								alive(target), reasonOf(target), want.dies)
						}
					case !alive(target):
						t.Fatalf("target died with %v", reasonOf(target))
					}
					select {
					case m := <-inbox:
						em, ok := m.(ExitMsg)
						if want.msg == nil || !ok || em.Reason != want.msg ||
							(tt.viaLink && em.From != from) {
							t.Errorf("unexpected message %#v", m)
						}
					default:
						if want.msg != nil {
							t.Errorf("no ExitMsg, want reason %v", want.msg)
						}
					}
				})
			})
		}
	}
}

func TestExitSelfNormal(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		p, _ := actor(n, func(s *Self) { s.Exit(s.PID(), Normal) })
		synctest.Wait()
		if alive(p) || reasonOf(p) != Normal {
			t.Errorf("exit(self(), normal): alive=%v reason=%v", alive(p), reasonOf(p))
		}
	})
}

func TestLinkIsRemovedByExitSignal(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		a, _ := actor(n, nil)
		b, inbox := actor(n, func(s *Self) {
			s.TrapExit(true)
			s.Link(a.pid)
		})
		n.Send(a.pid, exitWith{errBoom})
		synctest.Wait()
		<-inbox
		if linkCount(b) != 0 {
			t.Error("link survived the exit signal")
		}
	})
}

func TestLinkToDeadProcess(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		dead := n.spawn(func(*Self) error { return nil })
		synctest.Wait()

		p, inbox := actor(n, func(s *Self) {
			s.TrapExit(true)
			s.Link(dead.pid)
			remote := dead.pid
			remote.node = "other@host"
			s.Link(remote)
		})
		synctest.Wait()
		if m := <-inbox; m != (ExitMsg{From: dead.pid, Reason: NoProc}) {
			t.Errorf("got %#v", m)
		}
		if m := <-inbox; m.(ExitMsg).Reason != NoConnection {
			t.Errorf("got %#v", m)
		}
		if linkCount(p) != 0 {
			t.Error("link to a dead process was created")
		}

		q, _ := actor(n, func(s *Self) { s.Link(dead.pid) })
		synctest.Wait()
		if reasonOf(q) != NoProc {
			t.Errorf("non-trapping process: reason = %v, want noproc", reasonOf(q))
		}
	})
}

func TestUnlink(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		a, _ := actor(n, nil)
		b, _ := actor(n, func(s *Self) {
			s.Link(a.pid)
			s.Unlink(a.pid)
		})
		if linkCount(a) != 0 || linkCount(b) != 0 {
			t.Error("links remain after Unlink")
		}
		n.Send(a.pid, exitWith{errBoom})
		synctest.Wait()
		if !alive(b) {
			t.Errorf("unlinked process died with %v", reasonOf(b))
		}
	})
}

func TestLinkChain(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		const length = 100
		procs := make([]*process, length)
		procs[0], _ = actor(n, nil)
		for i := 1; i < length; i++ {
			prev := procs[i-1].pid
			procs[i], _ = actor(n, func(s *Self) { s.Link(prev) })
		}
		n.Send(procs[length-1].pid, exitWith{errBoom})
		synctest.Wait()
		for i, p := range procs {
			if reasonOf(p) != errBoom {
				t.Fatalf("procs[%d]: reason = %v", i, reasonOf(p))
			}
		}
	})
}

func TestSpawnLink(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		parent, _ := actor(n, func(s *Self) {
			s.SpawnLink(func(*Self) error { panic("crash at once") })
		})
		synctest.Wait()
		var pe *PanicError
		if !errors.As(reasonOf(parent), &pe) {
			t.Errorf("parent reason = %v", reasonOf(parent))
		}
	})
}

func TestSpawnLinkFromDeadParent(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		started := false
		parent, _ := actor(n, func(s *Self) {
			s.Exit(s.PID(), Kill) // now a zombie that still runs
			s.SpawnLink(func(*Self) error { started = true; return nil })
		})
		synctest.Wait()
		if reasonOf(parent) != Killed {
			t.Errorf("parent reason = %v", reasonOf(parent))
		}
		if started {
			t.Error("child of a dead parent ran")
		}
	})
}

// TestLinkRace links many processes to one that dies concurrently. Each must
// learn about the death exactly once: either through the link or as NoProc.
func TestLinkRace(t *testing.T) {
	const linkers = 200
	n := NewNode("")
	target, _ := actor(n, nil)

	start := make(chan struct{})
	inboxes := make([]chan ExitMsg, linkers)
	for i := range linkers {
		inbox := make(chan ExitMsg, 2)
		inboxes[i] = inbox
		n.spawn(func(s *Self) error {
			s.TrapExit(true)
			<-start
			s.Link(target.pid)
			for {
				msg, err := s.Receive(context.Background())
				if err != nil {
					return err
				}
				inbox <- msg.(ExitMsg)
			}
		})
	}
	close(start)
	n.Send(target.pid, exitWith{errBoom})

	timeout := time.After(5 * time.Second)
	for i, inbox := range inboxes {
		select {
		case m := <-inbox:
			if m.From != target.pid || (m.Reason != errBoom && m.Reason != NoProc) {
				t.Fatalf("linker %d: got %#v", i, m)
			}
		case <-timeout:
			t.Fatalf("linker %d never learned that the target died", i)
		}
	}
	for i, inbox := range inboxes {
		select {
		case m := <-inbox:
			t.Fatalf("linker %d: duplicate %#v", i, m)
		default:
		}
	}
}
