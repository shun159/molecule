package proc

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestWatch(t *testing.T) {
	for _, reason := range []error{Normal, errBoom} {
		t.Run(reason.Error(), func(t *testing.T) {
			bubble(t, func(t *testing.T, n *Node) {
				target, _ := actor(n, nil)
				ctx, stop := n.Watch(context.Background(), target.pid)
				defer stop()

				synctest.Wait()
				if ctx.Err() != nil {
					t.Fatalf("watch fired early: %v", context.Cause(ctx))
				}
				n.Send(target.pid, exitWith{reason})
				<-ctx.Done()
				if c := context.Cause(ctx); c != reason {
					t.Errorf("cause = %v, want %v", c, reason)
				}
			})
		})
	}
}

func TestWatchKilled(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		ctx, stop := n.Watch(context.Background(), target.pid)
		defer stop()
		do(n, func(s *Self) { s.Exit(target.pid, Kill) })
		<-ctx.Done()
		if c := context.Cause(ctx); c != Killed {
			t.Errorf("cause = %v, want killed", c)
		}
	})
}

func TestWatchNoProc(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		dead := n.spawn(func(*Self) error { return nil })
		synctest.Wait()
		remote := dead.pid
		remote.node = "other@host"

		for pid, want := range map[PID]error{dead.pid: NoProc, remote: NoConnection} {
			ctx, stop := n.Watch(context.Background(), pid)
			if c := context.Cause(ctx); c != want {
				t.Errorf("Watch(%v): cause = %v, want %v", pid, c, want)
			}
			stop()
			if c := context.Cause(ctx); c != want {
				t.Errorf("stop changed the cause to %v", c)
			}
		}
	})
}

func TestWatchStop(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		ctx, stop := n.Watch(context.Background(), target.pid)
		stop()
		if c := context.Cause(ctx); c != context.Canceled {
			t.Errorf("cause after stop = %v", c)
		}
		if monitors, _ := monitorCounts(target); monitors != 0 {
			t.Error("target still tracks a stopped watch")
		}
		n.Send(target.pid, exitWith{errBoom})
		synctest.Wait()
		if c := context.Cause(ctx); c != context.Canceled {
			t.Errorf("death after stop changed the cause to %v", c)
		}
		stop() // idempotent
	})
}

func TestWatchStopAfterDeath(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		ctx, stop := n.Watch(context.Background(), target.pid)
		n.Send(target.pid, exitWith{errBoom})
		<-ctx.Done()
		stop()
		if c := context.Cause(ctx); c != errBoom {
			t.Errorf("stop after death changed the cause to %v", c)
		}
	})
}

func TestWatchParent(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		parent, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		ctx, stop := n.Watch(parent, target.pid)

		start := time.Now()
		<-ctx.Done()
		if c := context.Cause(ctx); !errors.Is(c, context.DeadlineExceeded) || time.Since(start) != time.Second {
			t.Errorf("cause = %v after %v", c, time.Since(start))
		}
		if !alive(target) {
			t.Error("target died")
		}
		stop()
		if monitors, _ := monitorCounts(target); monitors != 0 {
			t.Error("target still tracks a stopped watch")
		}
	})
}

// TestWatchRace watches processes that die concurrently. Every watch must
// fire, with the real reason or NoProc.
func TestWatchRace(t *testing.T) {
	const rounds, watches = 50, 50
	n := NewNode("")
	for round := range rounds {
		target, _ := actor(n, nil)
		ctxs := make(chan context.Context, watches)
		start := make(chan struct{})
		for range watches {
			go func() {
				<-start
				ctx, stop := n.Watch(context.Background(), target.pid)
				context.AfterFunc(ctx, stop)
				ctxs <- ctx
			}()
		}
		close(start)
		n.Send(target.pid, exitWith{errBoom})

		timeout := time.After(5 * time.Second)
		for i := range watches {
			ctx := <-ctxs
			select {
			case <-ctx.Done():
				if c := context.Cause(ctx); c != errBoom && c != NoProc {
					t.Fatalf("round %d, watch %d: cause = %v", round, i, c)
				}
			case <-timeout:
				t.Fatalf("round %d, watch %d never fired", round, i)
			}
		}
	}
}

// TestWatchAndMonitor checks that both kinds of watchers on one process
// are notified.
func TestWatchAndMonitor(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		target, _ := actor(n, nil)
		var ref Ref
		_, inbox := actor(n, func(s *Self) { ref = s.Monitor(target.pid) })
		ctx, stop := n.Watch(context.Background(), target.pid)
		defer stop()

		n.Send(target.pid, exitWith{errBoom})
		<-ctx.Done()
		if m := <-inbox; m != (DownMsg{Ref: ref, PID: target.pid, Reason: errBoom}) {
			t.Errorf("got %#v", m)
		}
	})
}
