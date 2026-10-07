package proc

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

var errInit = errors.New("init failed")

// waitForever is a process body that only stops when it is killed.
func waitForever(s *Self) error {
	for {
		if _, err := s.Receive(context.Background()); err != nil {
			return err
		}
	}
}

// starter runs fn in a process that traps exits as requested, and returns
// what fn returned together with the starter process.
func starter[T any](n *Node, trap bool, fn func(*Self) T) (T, *process) {
	got := make(chan T, 1)
	p, _ := actor(n, func(s *Self) {
		s.TrapExit(trap)
		got <- fn(s)
	})
	return <-got, p
}

func linked(a, b *process) bool {
	unlock := lockPair(a, b)
	defer unlock()
	_, ab := a.links[b.pid]
	_, ba := b.links[a.pid]
	return ab && ba
}

func TestStartLink(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		type result struct {
			pid    PID
			err    error
			parent PID
		}
		parents := make(chan PID, 1)
		r, caller := starter(n, false, func(s *Self) result {
			pid, err := s.StartLink(context.Background(), func(s *Self) error {
				parents <- s.Parent()
				s.InitAck(nil)
				return waitForever(s)
			})
			return result{pid, err, s.PID()}
		})
		if r.err != nil {
			t.Fatal(r.err)
		}
		child := n.lookup(r.pid)
		if child == nil || !alive(child) {
			t.Fatal("child not alive")
		}
		if !linked(caller, child) {
			t.Error("child not linked to caller")
		}
		if p := <-parents; p != r.parent {
			t.Errorf("Parent() = %v, want %v", p, r.parent)
		}
	})
}

func TestStartLinkAckError(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		type result struct {
			pid PID
			err error
		}
		// Like gen_server's ignore: report a failure, then exit normally.
		r, caller := starter(n, false, func(s *Self) result {
			pid, err := s.StartLink(context.Background(), func(s *Self) error {
				s.InitAck(errInit)
				return nil
			})
			return result{pid, err}
		})
		if r.err != errInit || !r.pid.IsZero() {
			t.Errorf("StartLink = %v, %v; want zero PID, %v", r.pid, r.err, errInit)
		}
		synctest.Wait()
		if !alive(caller) {
			t.Errorf("caller died with %v", reasonOf(caller))
		}
	})
}

func TestStartLinkExitBeforeAck(t *testing.T) {
	tests := []struct {
		name  string
		fn    func(*Self) error
		check func(error) bool
		// abnormal exits kill a caller that does not trap exits
		abnormal bool
	}{
		{"error", func(*Self) error { return errInit }, func(e error) bool { return e == errInit }, true},
		{"panic", func(*Self) error { panic("in init") }, func(e error) bool {
			var pe *PanicError
			return errors.As(e, &pe)
		}, true},
		{"normal", func(*Self) error { return nil }, func(e error) bool { return e == Normal }, false},
	}
	for _, tt := range tests {
		for _, trap := range []bool{true, false} {
			name := tt.name + map[bool]string{true: "/trap", false: "/notrap"}[trap]
			t.Run(name, func(t *testing.T) {
				bubble(t, func(t *testing.T, n *Node) {
					type result struct {
						err  error
						left int
					}
					r, caller := starter(n, trap, func(s *Self) result {
						_, err := s.StartLink(context.Background(), tt.fn)
						return result{err, s.p.mbox.len()}
					})
					synctest.Wait()
					if trap || !tt.abnormal {
						if !tt.check(r.err) {
							t.Errorf("StartLink error = %v", r.err)
						}
						if r.left != 0 {
							t.Errorf("%d messages left in the caller's mailbox", r.left)
						}
						if !alive(caller) {
							t.Errorf("caller died with %v", reasonOf(caller))
						}
					} else if !tt.check(reasonOf(caller)) {
						t.Errorf("caller reason = %v, want the child's", reasonOf(caller))
					}
				})
			})
		}
	}
}

func TestStartLinkTimeout(t *testing.T) {
	for _, trap := range []bool{true, false} {
		t.Run(map[bool]string{true: "trap", false: "notrap"}[trap], func(t *testing.T) {
			bubble(t, func(t *testing.T, n *Node) {
				type result struct {
					pid     PID
					err     error
					elapsed time.Duration
				}
				children := make(chan *process, 1)
				r, caller := starter(n, trap, func(s *Self) result {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					start := time.Now()
					pid, err := s.StartLink(ctx, func(c *Self) error {
						children <- c.p
						return waitForever(c) // never acks
					})
					return result{pid, err, time.Since(start)}
				})
				if !errors.Is(r.err, context.DeadlineExceeded) || r.elapsed != time.Second || !r.pid.IsZero() {
					t.Errorf("StartLink = %v, %v after %v", r.pid, r.err, r.elapsed)
				}
				child := <-children
				synctest.Wait()
				if reasonOf(child) != Killed {
					t.Errorf("child reason = %v, want killed", reasonOf(child))
				}
				if !alive(caller) {
					t.Errorf("caller died with %v", reasonOf(caller))
				}
				if l := caller.mbox.len(); l != 0 {
					t.Errorf("%d messages left in the caller's mailbox", l)
				}
			})
		})
	}
}

// TestStartLinkAckThenDie checks that an acknowledged start succeeds even
// when the child dies right after.
func TestStartLinkAckThenDie(t *testing.T) {
	for range 200 {
		bubble(t, func(t *testing.T, n *Node) {
			err, _ := starter(n, true, func(s *Self) error {
				_, err := s.StartLink(context.Background(), func(s *Self) error {
					s.InitAck(nil)
					return errBoom
				})
				return err
			})
			if err != nil {
				t.Errorf("StartLink = %v, want success", err)
			}
		})
		if t.Failed() {
			return
		}
	}
}

func TestStartLinkCallerKilled(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		children := make(chan *process, 1)
		errc := make(chan error, 1)
		caller := n.spawn(func(s *Self) error {
			_, err := s.StartLink(context.Background(), func(c *Self) error {
				children <- c.p
				return waitForever(c)
			})
			errc <- err
			return err
		})
		child := <-children
		do(n, func(s *Self) { s.Exit(caller.pid, Kill) })
		if err := <-errc; err != Killed {
			t.Errorf("StartLink = %v, want killed", err)
		}
		synctest.Wait()
		if reasonOf(child) != Killed {
			t.Errorf("child reason = %v, want killed through the link", reasonOf(child))
		}
	})
}

func TestNodeStart(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		pid, err := n.Start(context.Background(), func(s *Self) error {
			if !s.Parent().IsZero() {
				t.Errorf("Parent() = %v", s.Parent())
			}
			s.InitAck(nil)
			return waitForever(s)
		})
		if err != nil || !n.IsAlive(pid) {
			t.Errorf("Start = %v, %v", pid, err)
		}

		if _, err := n.Start(context.Background(), func(*Self) error { return errInit }); err != errInit {
			t.Errorf("failing Start = %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := n.Start(ctx, waitForever); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("timed out Start = %v", err)
		}
	})
}

func TestInitAckOutsideStart(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		p, _ := actor(n, func(s *Self) {
			s.InitAck(nil) // not started by Start: no effect
		})
		_, err := n.Start(context.Background(), func(s *Self) error {
			s.InitAck(nil)
			s.InitAck(errInit) // only the first counts
			return waitForever(s)
		})
		if err != nil {
			t.Errorf("Start = %v", err)
		}
		if !alive(p) {
			t.Errorf("process died with %v", reasonOf(p))
		}
	})
}

func TestSpawnParent(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		parents := make(chan PID, 2)
		var self PID
		do(n, func(s *Self) {
			self = s.PID()
			s.Spawn(func(c *Self) error { parents <- c.Parent(); return nil })
			s.SpawnLink(func(c *Self) error { parents <- c.Parent(); return nil })
		})
		for range 2 {
			if p := <-parents; p != self {
				t.Errorf("Parent() = %v, want %v", p, self)
			}
		}
		n.Spawn(func(c *Self) error { parents <- c.Parent(); return nil })
		if p := <-parents; !p.IsZero() {
			t.Errorf("Parent() of Node.Spawn = %v", p)
		}
	})
}

// TestStartLinkImmediateDeath starts children that fail at once, so they
// may die before StartLink would otherwise get to watch them. The real
// reason must still be reported, never NoProc.
func TestStartLinkImmediateDeath(t *testing.T) {
	n := NewNode("")
	errs := make(chan error)
	n.spawn(func(s *Self) error {
		s.TrapExit(true)
		for range 2000 {
			_, err := s.StartLink(context.Background(), func(*Self) error { return errInit })
			errs <- err
		}
		close(errs)
		return nil
	})
	for err := range errs {
		if err != errInit {
			t.Fatalf("StartLink = %v, want %v", err, errInit)
		}
	}
}

// TestStartLinkCallerKilledTrappingChild kills the caller of StartLink
// while the child, trapping exits, survives the link. StartLink must still
// return at once rather than wait for an ack that may never come.
func TestStartLinkCallerKilledTrappingChild(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		children := make(chan *process, 1)
		errc := make(chan error, 1)
		caller := n.spawn(func(s *Self) error {
			_, err := s.StartLink(context.Background(), func(c *Self) error {
				c.TrapExit(true)
				children <- c.p
				return waitForever(c)
			})
			errc <- err
			return err
		})
		child := <-children
		do(n, func(s *Self) { s.Exit(caller.pid, Kill) })
		if err := <-errc; err != Killed {
			t.Errorf("StartLink = %v, want killed", err)
		}
		synctest.Wait()
		if !alive(child) {
			t.Errorf("trapping child died with %v", reasonOf(child))
		}
	})
}
