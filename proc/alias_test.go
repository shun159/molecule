package proc

import (
	"testing"
	"time"
)

func noMsg(t *testing.T, a Alias) {
	t.Helper()
	select {
	case m := <-a.C:
		t.Errorf("unexpected %+v", m)
	default:
	}
}

func TestAlias(t *testing.T) {
	n := NewNode("")
	a := n.Alias()
	defer a.Release()

	n.SendAlias(a.Ref, "first")
	if m := <-a.C; m.Msg != "first" || m.Down {
		t.Errorf("got %+v", m)
	}
	n.SendAlias(a.Ref, "second") // the alias is gone after the first
	noMsg(t, a)
	if n.aliases.len() != 0 {
		t.Error("alias still active after delivery")
	}
}

func TestAliasRelease(t *testing.T) {
	n := NewNode("")
	a := n.Alias()
	a.Release()
	n.SendAlias(a.Ref, "late")
	noMsg(t, a)
	if n.aliases.len() != 0 {
		t.Error("alias not released")
	}
	a.Release() // idempotent
}

func TestAliasForeign(t *testing.T) {
	n := NewNode("a@host")
	other := NewNode("b@host")
	a := other.Alias()
	defer a.Release()

	n.SendAlias(a.Ref, "remote") // TODO(dist): dropped for now
	stale := a.Ref
	stale.creation++
	other.SendAlias(stale, "stale")
	noMsg(t, a)
}

func TestMonitorAliasDown(t *testing.T) {
	n := NewNode("")
	p, _ := actor(n, nil)
	a := n.MonitorAlias(p.pid)
	defer a.Release()

	n.Send(p.pid, exitWith{errBoom})
	if m := <-a.C; !m.Down || m.Reason != errBoom || m.Msg != nil {
		t.Errorf("got %+v", m)
	}
	n.SendAlias(a.Ref, "after death")
	noMsg(t, a)
	if n.aliases.len() != 0 {
		t.Error("alias still active after the death")
	}
}

func TestMonitorAliasNoProc(t *testing.T) {
	n := NewNode("a@host")
	dead := n.spawn(func(*Self) error { return nil })
	<-dead.done
	remote := dead.pid
	remote.node = "b@host"

	for pid, want := range map[PID]error{dead.pid: NoProc, remote: NoConnection} {
		a := n.MonitorAlias(pid)
		select {
		case m := <-a.C:
			if !m.Down || m.Reason != want {
				t.Errorf("MonitorAlias(%v): got %+v, want down with %v", pid, m, want)
			}
		default:
			t.Errorf("MonitorAlias(%v): no immediate down", pid)
		}
		a.Release()
	}
	if n.aliases.len() != 0 {
		t.Error("aliases left behind")
	}
}

// TestMonitorAliasReplyThenDeath checks that a reply sent before dying is
// what arrives, and only it.
func TestMonitorAliasReplyThenDeath(t *testing.T) {
	n := NewNode("")
	for range 200 {
		ack := make(chan Ref, 1)
		p := n.spawn(func(s *Self) error {
			s.Node().SendAlias(<-ack, "reply")
			return errBoom
		})
		a := n.MonitorAlias(p.pid)
		ack <- a.Ref
		<-p.done
		if m := <-a.C; m.Down || m.Msg != "reply" {
			t.Fatalf("got %+v, want the reply", m)
		}
		noMsg(t, a)
		a.Release()
	}
}

func TestMonitorAliasRelease(t *testing.T) {
	n := NewNode("")
	p, _ := actor(n, nil)
	a := n.MonitorAlias(p.pid)
	a.Release()
	if monitors, _ := monitorCounts(p); monitors != 0 {
		t.Error("target still tracks a released alias")
	}
	n.Send(p.pid, exitWith{errBoom})
	<-p.done
	time.Sleep(time.Millisecond)
	noMsg(t, a)
}

// TestMonitorAliasRace monitors processes that die concurrently. Each alias
// must get exactly one message: the death, with the real reason or NoProc.
func TestMonitorAliasRace(t *testing.T) {
	const rounds, watchers = 50, 50
	n := NewNode("")
	for round := range rounds {
		target, _ := actor(n, nil)
		aliases := make(chan Alias, watchers)
		start := make(chan struct{})
		for range watchers {
			go func() {
				<-start
				aliases <- n.MonitorAlias(target.pid)
			}()
		}
		close(start)
		n.Send(target.pid, exitWith{errBoom})

		timeout := time.After(5 * time.Second)
		for i := range watchers {
			a := <-aliases
			select {
			case m := <-a.C:
				if !m.Down || (m.Reason != errBoom && m.Reason != NoProc) {
					t.Fatalf("round %d, alias %d: got %+v", round, i, m)
				}
			case <-timeout:
				t.Fatalf("round %d, alias %d: no message", round, i)
			}
			a.Release()
		}
	}
	if l := n.aliases.len(); l != 0 {
		t.Errorf("%d aliases left behind", l)
	}
}

func TestPIDWhereIs(t *testing.T) {
	n := NewNode("")
	pid := n.NewPIDForTest(42)
	if got, ok := pid.WhereIs(n); !ok || got != pid {
		t.Errorf("WhereIs = %v, %v", got, ok)
	}
	if _, ok := (PID{}).WhereIs(n); ok {
		t.Error("zero PID resolved")
	}
	if n.Node() != n {
		t.Error("Node() is not the node itself")
	}
}
