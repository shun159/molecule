package dist

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"syscall"
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

// TestSilentHeard has a node say nothing for a while, then tick: it is
// told silent, then heard, its connection kept. Trickling a frame for as
// long, it is heard throughout.
func TestSilentHeard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	say := make(chan []byte)
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
		go io.Copy(io.Discard, conn)
		for bytes := range say {
			if _, err := conn.Write(bytes); err != nil {
				return
			}
		}
	}()
	defer close(say)

	n := proc.NewNode("a@test")
	const silent = 100 * time.Millisecond
	d, err := Start(n, Config{
		Cookie:   "c",
		Resolve:  func(string) (string, bool) { return ln.Addr().String(), true },
		TickTime: time.Minute,
		Silent:   silent,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	events := make(chan any, 16)
	n.Spawn(func(s *proc.Self) error {
		s.MonitorNodes(true)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			events <- msg
		}
	})
	if err := d.Connect(context.Background(), "b@test"); err != nil {
		t.Fatal(err)
	}
	next := func(want any, within time.Duration) {
		t.Helper()
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("got %#v, want %#v", got, want)
			}
		case <-time.After(within):
			t.Fatalf("no %#v within %v", want, within)
		}
	}
	next(proc.NodeUp{Node: "b@test"}, time.Second)
	start := time.Now()
	next(proc.NodeSilent{Node: "b@test"}, 5*silent)
	if e := time.Since(start); e < silent/2 {
		t.Errorf("silent after %v only", e)
	}
	say <- packet(appendFrame(nil, frame{op: opTick}))
	next(proc.NodeHeard{Node: "b@test"}, 5*silent)

	// A frame of 64 KiB, a KiB at a time, over four times Silent.
	payload, err := gobCodec{}.NewEncoder().Encode(strings.Repeat("x", 64<<10))
	if err != nil {
		t.Fatal(err)
	}
	big := packet(appendFrame(nil, frame{op: opSendName, name: "nobody", payload: payload}))
	every := 4 * silent / time.Duration(len(big)/1024)
	for len(big) > 0 {
		k := min(1024, len(big))
		say <- big[:k]
		big = big[k:]
		time.Sleep(every)
	}
	select {
	case got := <-events:
		t.Errorf("amid a frame: %#v", got)
	default:
	}
	if got := d.Nodes(); len(got) != 1 {
		t.Errorf("connected to %v", got)
	}
}

// packet prefixes f with its length, as the socket does.
func packet(f []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(f))), f...)
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

// TestPeerKilled kills the process of a connection: the links through it
// fail, as when the connection is lost, and the next send connects anew.
func TestPeerKilled(t *testing.T) {
	a, b := proc.NewNode("a@test"), proc.NewNode("b@test")
	db, err := Start(b, Config{Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Stop()
	addr := db.Addr().String()
	da, err := Start(a, Config{Resolve: func(string) (string, bool) { return addr, true }})
	if err != nil {
		t.Fatal(err)
	}
	defer da.Stop()

	got := make(chan any, 8)
	ready := make(chan struct{})
	pb := b.Spawn(func(s *proc.Self) error {
		s.TrapExit(true)
		close(ready)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg
		}
	})
	<-ready
	gotA := make(chan any, 8)
	a.Spawn(func(s *proc.Self) error {
		s.TrapExit(true)
		s.Link(pb)
		s.Send(pb, "linked")
		msg, err := s.Receive(context.Background())
		gotA <- msg
		return err
	})
	if m := <-got; m != "linked" {
		t.Fatalf("got %#v", m)
	}

	da.mu.Lock()
	peer := da.peers["b@test"].pid
	da.mu.Unlock()
	a.Spawn(func(s *proc.Self) error { s.Exit(peer, proc.Kill); return nil })
	select {
	case m := <-got:
		if e, ok := m.(proc.ExitMsg); !ok || e.Reason != proc.NoConnection {
			t.Errorf("got %#v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the link did not fail at b")
	}
	select {
	case m := <-gotA:
		if e, ok := m.(proc.ExitMsg); !ok || e.Reason != proc.NoConnection {
			t.Errorf("at a: got %#v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the link did not fail at a")
	}
	a.Send(pb, "again")
	select {
	case m := <-got:
		if m != "again" {
			t.Errorf("got %#v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not connected again")
	}
}

// stuck listens for a node b@test that does not read: past the handshake,
// if shake, or from the start.
func stuck(t *testing.T, shake bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() { close(done); ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if shake {
			b := hello{name: "b@test", creation: 1, challenge: newChallenge()}
			if _, err := acceptHandshake(conn, b, "c", func(peerInfo) string { return "" }); err != nil {
				return
			}
		}
		<-done
	}()
	return ln.Addr().String()
}

// TestMaxQueue sends to a node past MaxQueue, while connecting and behind
// a node that does not read: the connection is dropped, the monitor
// through it fails.
func TestMaxQueue(t *testing.T) {
	big := strings.Repeat("x", 1<<20)
	for _, shake := range []bool{false, true} {
		addr := stuck(t, shake)
		n := proc.NewNode("a@test")
		d, err := Start(n, Config{
			Cookie:   "c",
			Resolve:  func(string) (string, bool) { return addr, true },
			MaxQueue: 4,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Stop()
		if shake {
			if err := d.Connect(context.Background(), "b@test"); err != nil {
				t.Fatal(err)
			}
		}
		pb := proc.NewNode("b@test").NewPID()
		ctx, stop := n.Watch(context.Background(), pb)
		defer stop()
		for range 200 {
			n.Send(pb, big)
		}
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("connected %v: connection kept", shake)
		}
		if err := context.Cause(ctx); err != proc.NoConnection {
			t.Errorf("connected %v: cause %v", shake, err)
		}
	}
}

// A connection counting its reads stays a TCP one, for the socket to
// write a frame's buffers at once.
func TestCountedTCP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if c, _ := counted(conn); !isSyscallConn(c) {
		t.Errorf("%T is no syscall.Conn", c)
	}
}

func isSyscallConn(c net.Conn) bool {
	_, ok := c.(syscall.Conn)
	return ok
}

// TestStaleDial has a dial end after the connection it was for was
// dropped, as the dial of two nodes connecting at once may (#89). The node
// dialed took it for its connection: it is kept, and what comes on it
// arrives.
func TestStaleDial(t *testing.T) {
	addrs := map[string]string{}
	resolve := func(node string) (string, bool) { addr, ok := addrs[node]; return addr, ok }
	a, b := proc.NewNode("a@test"), proc.NewNode("b@test")
	var ds []*Dist
	for _, n := range []*proc.Node{a, b} {
		d, err := Start(n, Config{Listen: "127.0.0.1:0", Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Stop()
		addrs[n.Name()] = d.Addr().String()
		ds = append(ds, d)
	}
	da, db := ds[0], ds[1]
	got := make(chan any, 8)
	pb := b.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg
		}
	})
	if err := db.Connect(context.Background(), "a@test"); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	gone := db.peers["a@test"]
	db.mu.Unlock()
	db.Disconnect("a@test")
	// a has a peer of b since it accepted b, up or not yet.
	until(t, "a forgets b", func() bool {
		da.mu.Lock()
		defer da.mu.Unlock()
		return da.peers["b@test"] == nil
	})

	db.dial(gone)
	until(t, "b connected again", func() bool { return len(db.Nodes()) == 1 })
	a.Send(pb, "again")
	select {
	case m := <-got:
		if m != "again" {
			t.Errorf("got %#v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing received")
	}
}

// until waits for cond to hold, for 5 seconds at most.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("not so: %s", what)
		}
	}
}
