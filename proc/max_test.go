package proc

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
)

// A process with more messages waiting than its limit is killed, reported,
// its mailbox dropped; signals count as messages do.
func TestMaxMessages(t *testing.T) {
	for _, signal := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			var log bytes.Buffer
			n := NewNode("a@host", WithLogger(slog.New(slog.NewTextHandler(&log, nil))))
			release := make(chan struct{})
			var prev [2]int
			p := n.spawn(func(s *Self) error {
				prev[0] = s.SetMaxMessages(5)
				prev[1] = s.SetMaxMessages(3)
				s.TrapExit(true)
				<-release // not receiving
				_, err := s.Receive(context.Background())
				return err
			})
			if err := n.Register("slow", p.pid); err != nil {
				t.Fatal(err)
			}
			_, inbox := actor(n, func(s *Self) { s.Monitor(p.pid) })
			synctest.Wait()
			if prev != [2]int{0, 5} {
				t.Errorf("SetMaxMessages returned %v, want [0 5]", prev)
			}

			for i := range 3 {
				n.Send(p.pid, i)
			}
			synctest.Wait()
			if !alive(p) || p.mbox.len() != 3 {
				t.Fatalf("at the limit: alive %v, %d waiting", alive(p), p.mbox.len())
			}
			if signal {
				do(n, func(s *Self) { s.Exit(p.pid, errBoom) }) // an ExitMsg
			} else {
				n.Send(p.pid, 3)
			}
			synctest.Wait()
			if reasonOf(p) != MailboxFull {
				t.Fatalf("past the limit: reason %v", reasonOf(p))
			}
			if m := (<-inbox).(DownMsg); m.Reason != MailboxFull {
				t.Errorf("Down %v", m.Reason)
			}
			if p.mbox.len() != 0 {
				t.Errorf("%d messages kept", p.mbox.len())
			}
			if _, ok := n.WhereIs("slow"); ok {
				t.Error("name kept")
			}
			if s := log.String(); !strings.Contains(s, "mailbox full") || !strings.Contains(s, "registered_name=slow") || !strings.Contains(s, "message_queue_len=4") {
				t.Errorf("report: %s", s)
			}
			close(release)
			synctest.Wait()
			for _, p := range n.procs.all() {
				p.die(Killed)
			}
		})
	}
}

// Without a limit, or with it taken off, a mailbox grows.
func TestMaxMessagesOff(t *testing.T) {
	bubble(t, func(t *testing.T, n *Node) {
		release := make(chan struct{})
		p := n.spawn(func(s *Self) error {
			s.SetMaxMessages(1)
			s.SetMaxMessages(0)
			<-release
			return nil
		})
		synctest.Wait()
		for i := range 100 {
			n.Send(p.pid, i)
		}
		synctest.Wait()
		if !alive(p) || p.mbox.len() != 100 {
			t.Errorf("alive %v, %d waiting", alive(p), p.mbox.len())
		}
		close(release)
	})
}
