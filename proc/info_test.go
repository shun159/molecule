package proc

import (
	"context"
	"errors"
	"slices"
	"testing"
)

func TestInfo(t *testing.T) {
	n := NewNode("")
	block := make(chan struct{})
	parent := n.Spawn(func(s *Self) error {
		s.SetLabel("the parent")
		child, err := s.StartLink(context.Background(), func(c *Self) error {
			c.InitAck(nil)
			<-block
			return nil
		})
		if err != nil {
			return err
		}
		s.TrapExit(true)
		s.Monitor(child)
		n.Register("parent", s.PID())
		n.Send(s.PID(), "one")
		n.Send(s.PID(), "two")
		<-block
		return nil
	})
	eventually(t, func() bool {
		info, ok := n.Info(parent)
		return ok && info.MessageQueueLen == 2
	})
	info, _ := n.Info(parent)
	if info.Label != "the parent" || info.Name != "parent" || !info.TrapExit || len(info.Links) != 1 || info.Monitoring != 1 {
		t.Errorf("info %+v", info)
	}
	child := info.Links[0]
	ci, ok := n.Info(child)
	if !ok || ci.Parent != parent || ci.MonitoredBy != 1 || !slices.Equal(ci.Links, []PID{parent}) {
		t.Errorf("child info %+v", ci)
	}
	if got := n.Processes(); !slices.Equal(got, []PID{parent, child}) {
		t.Errorf("processes %v", got)
	}

	failing := n.Spawn(func(*Self) error { return errors.New("boom") })
	eventually(t, func() bool { return !n.IsAlive(failing) })
	close(block)
	eventually(t, func() bool { return len(n.Processes()) == 0 })
	if st := n.Stats(); st != (NodeStats{Processes: 0, Spawned: 3, Exited: 3, Crashed: 1}) {
		t.Errorf("stats %+v", st)
	}
	if _, ok := n.Info(parent); ok {
		t.Error("info of a dead process")
	}
}
