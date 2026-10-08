package dist

import (
	"context"
	"io"
	"net"
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
	conn := &connection{d: d, p: &peer{node: "b@test"}, dec: gobCodec{}.NewDecoder()}
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

// TestSilentNode connects to a node that completes the handshake, then
// says nothing: after TickTime, the connection is taken for lost.
func TestSilentNode(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		b := hello{name: "b@test", creation: 1, challenge: newChallenge()}
		if _, err := acceptHandshake(conn, b, "c", func(peerInfo) string { return "" }); err != nil {
			return
		}
		io.Copy(io.Discard, conn)
	}()
	n := proc.NewNode("a@test")
	const tickTime = 200 * time.Millisecond
	d, err := Start(n, Config{
		Cookie:   "c",
		Resolve:  func(string) (string, bool) { return ln.Addr().String(), true },
		TickTime: tickTime,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	if err := d.Connect(context.Background(), "b@test"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ctx, stop := n.Watch(context.Background(), proc.NewNode("b@test").NewPID())
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * tickTime):
		t.Fatal("silent connection kept")
	}
	if err := context.Cause(ctx); err != proc.NoConnection {
		t.Errorf("cause %v", err)
	}
	if e := time.Since(start); e < tickTime/2 {
		t.Errorf("dropped after %v only", e)
	}
}

// TestIdleConnection keeps two idle nodes connected: ticks show they
// live.
func TestIdleConnection(t *testing.T) {
	const tickTime = 100 * time.Millisecond
	addrs := map[string]string{}
	var nodes []*proc.Node
	var dists []*Dist
	for _, name := range []string{"a@test", "b@test"} {
		n := proc.NewNode(name)
		d, err := Start(n, Config{
			Listen:   "127.0.0.1:0",
			Cookie:   "c",
			Resolve:  func(node string) (string, bool) { a, ok := addrs[node]; return a, ok },
			TickTime: tickTime,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Stop()
		addrs[name] = d.Addr().String()
		nodes, dists = append(nodes, n), append(dists, d)
	}
	if err := dists[0].Connect(context.Background(), "b@test"); err != nil {
		t.Fatal(err)
	}
	pb := nodes[1].Spawn(func(s *proc.Self) error {
		_, err := s.Receive(context.Background())
		return err
	})
	ctx, stop := nodes[0].Watch(context.Background(), pb)
	defer stop()
	time.Sleep(6 * tickTime)
	if ctx.Err() != nil {
		t.Fatalf("connection lost: %v", context.Cause(ctx))
	}
	if got := dists[0].Nodes(); len(got) != 1 {
		t.Errorf("connected to %v", got)
	}
}
