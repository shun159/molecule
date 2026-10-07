package dist

import (
	"context"
	"testing"
	"time"

	"github.com/shun159/molecule/proc"
)

// TestForgedFrames has the connection to b receive exit signals from a
// process of c, which are dropped, and of b, which are not.
func TestForgedFrames(t *testing.T) {
	n := proc.NewNode("a@test")
	d, err := Start(n, Config{})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan any, 8)
	local := n.Spawn(func(s *proc.Self) error {
		s.TrapExit(true)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg
		}
	})
	n.Send(local, "ready") // trapping exits
	<-got
	b, c := proc.NewNode("b@test"), proc.NewNode("c@test")
	conn := &connection{d: d, p: &peer{node: "b@test"}, dec: Gob.NewDecoder()}
	for _, from := range []proc.PID{c.NewPID(), b.NewPID()} {
		conn.receive(appendFrame(nil, frame{op: opExit, from: from, to: local, reason: proc.Shutdown}))
	}
	// Only b's exit signal arrives.
	select {
	case m := <-got:
		if e, ok := m.(proc.ExitMsg); !ok || e.From.Node() != "b@test" {
			t.Errorf("got %#v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("nothing")
	}
	select {
	case m := <-got:
		t.Errorf("got %#v", m)
	case <-time.After(30 * time.Millisecond):
	}
}
