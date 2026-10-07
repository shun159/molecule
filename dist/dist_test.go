package dist_test

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// cluster starts distributed nodes on loopback, which find each other by
// name.
func cluster(t *testing.T, cookie string, names ...string) ([]*proc.Node, []*dist.Dist) {
	t.Helper()
	var mu sync.Mutex
	addrs := map[string]string{}
	resolve := func(node string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		a, ok := addrs[node]
		return a, ok
	}
	var nodes []*proc.Node
	var dists []*dist.Dist
	for _, name := range names {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: cookie, Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(d.Stop)
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		nodes = append(nodes, n)
		dists = append(dists, d)
	}
	return nodes, dists
}

// inbox is a process trapping exits, handing what it receives to a
// channel, and running the functions sent to it.
func inbox(n *proc.Node) (proc.PID, <-chan any) {
	ch := make(chan any, 64)
	pid := n.Spawn(func(s *proc.Self) error {
		s.TrapExit(true)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if fn, ok := msg.(func(*proc.Self)); ok {
				fn(s)
				continue
			}
			ch <- msg
		}
	})
	return pid, ch
}

// in runs fn in the process pid, an inbox of n, and waits for it.
func in(n *proc.Node, pid proc.PID, fn func(*proc.Self)) {
	done := make(chan struct{})
	n.Send(pid, func(s *proc.Self) {
		fn(s)
		close(done)
	})
	<-done
}

func next(t *testing.T, ch <-chan any) any {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(5 * time.Second):
		t.Fatal("nothing received")
		return nil
	}
}

// Ping carries a PID, as messages often do.
type Ping struct {
	From proc.PID
	N    int
}

func init() { gob.Register(Ping{}) }

func TestSend(t *testing.T) {
	nodes, dists := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, cb := inbox(b)
	for i := range 100 {
		a.Send(pb, Ping{pa, i})
	}
	for i := range 100 {
		if m := next(t, cb); m != (Ping{pa, i}) {
			t.Fatalf("got %#v, want ping %d", m, i)
		}
	}
	// The PID that came is the PID of a process of a.
	b.Send(pa, "pong")
	if m := next(t, ca); m != "pong" {
		t.Errorf("got %#v", m)
	}
	b.Register("echo", pb)
	a.SendName("b@test", "echo", "by name")
	if m := next(t, cb); m != "by name" {
		t.Errorf("got %#v", m)
	}
	if got := dists[0].Nodes(); len(got) != 1 || got[0] != "b@test" {
		t.Errorf("a is connected to %v", got)
	}
}

// greeter is a genserver answering calls with a greeting.
type greeter struct{}

func (greeter) Init(proc.PID) (int, []gen.Effect, error) { return 0, nil, nil }

func (greeter) HandleCall(n int, name string, from genserver.From[string]) (int, []gen.Effect) {
	return n + 1, gen.Do(from.Reply(fmt.Sprintf("hello %s, #%d", name, n+1)))
}

func (greeter) HandleCast(n int, _ struct{}) (int, []gen.Effect) { return n, nil }

func TestCall(t *testing.T) {
	nodes, _ := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	ctx := context.Background()
	ref, err := genserver.Start(ctx, b, greeter{})
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := ref.Dest().WhereIs(b)
	remote := genserver.RefFor(greeter{}, pid)
	for i := range 3 {
		got, err := remote.Call(ctx, a, "a")
		if want := fmt.Sprintf("hello a, #%d", i+1); err != nil || got != want {
			t.Errorf("call: %q, %v; want %q", got, err, want)
		}
	}
	// A server gone, the call fails with its exit reason.
	ref.Stop(ctx, b)
	if _, err := remote.Call(ctx, a, "a"); !errors.Is(err, proc.NoProc) && !errors.Is(err, proc.Normal) {
		t.Errorf("call to a server gone: %v", err)
	}
}

func TestLinkAndMonitor(t *testing.T) {
	nodes, _ := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	crash := b.Spawn(func(s *proc.Self) error {
		if _, err := s.Receive(context.Background()); err != nil {
			return err
		}
		return fmt.Errorf("%w: done", proc.Shutdown)
	})
	var ref proc.Ref
	in(a, pa, func(s *proc.Self) {
		s.Link(crash)
		ref = s.Monitor(crash)
	})
	a.Send(crash, "go")
	var exited, down bool
	for range 2 {
		switch m := next(t, ca).(type) {
		case proc.ExitMsg:
			exited = m.From == crash && errors.Is(m.Reason, proc.Shutdown) && m.Reason.Error() == "shutdown: done"
		case proc.DownMsg:
			down = m.Ref == ref && m.PID == crash && errors.Is(m.Reason, proc.Shutdown)
		}
	}
	if !exited || !down {
		t.Errorf("exit %v, down %v", exited, down)
	}

	// Kill crosses nodes as such.
	victim, _ := inbox(b)
	ctx, stop := a.Watch(context.Background(), victim)
	defer stop()
	in(a, pa, func(s *proc.Self) { s.Exit(victim, proc.Kill) })
	<-ctx.Done()
	if err := context.Cause(ctx); err != proc.Killed {
		t.Errorf("killed with %v", err)
	}
}

func TestDisconnect(t *testing.T) {
	nodes, dists := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, cb := inbox(b)
	in(a, pa, func(s *proc.Self) { s.Link(pb) })
	in(b, pb, func(s *proc.Self) { s.Monitor(pa) })
	// The link and the monitor are in place once what follows them is
	// there.
	a.Send(pb, "sync")
	b.Send(pa, "sync")
	next(t, cb)
	next(t, ca)
	dists[0].Disconnect("b@test")
	if m := next(t, ca); m != (proc.ExitMsg{From: pb, Reason: proc.NoConnection}) {
		t.Errorf("at a: %#v", m)
	}
	var exited, down bool
	for range 2 {
		switch m := next(t, cb).(type) {
		case proc.ExitMsg:
			exited = m == proc.ExitMsg{From: pa, Reason: proc.NoConnection}
		case proc.DownMsg:
			down = m.PID == pa && m.Reason == proc.NoConnection
		}
	}
	if !exited || !down {
		t.Errorf("at b: exit %v, down %v", exited, down)
	}
	// Sending connects again.
	a.Send(pb, "again")
	if m := next(t, cb); m != "again" {
		t.Errorf("got %#v", m)
	}
}

func TestStopNode(t *testing.T) {
	nodes, dists := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, _ := inbox(b)
	var ref proc.Ref
	in(a, pa, func(s *proc.Self) { ref = s.Monitor(pb) })
	if err := dists[0].Connect(context.Background(), "b@test"); err != nil {
		t.Fatal(err)
	}
	dists[1].Stop()
	if m := next(t, ca); m != (proc.DownMsg{Ref: ref, PID: pb, Reason: proc.NoConnection}) {
		t.Errorf("got %#v", m)
	}
	if err := dists[0].Connect(context.Background(), "b@test"); err == nil {
		t.Error("connected to a stopped node")
	}
}

func TestBadCookie(t *testing.T) {
	nodes, dists := clusterWith(t, map[string]string{"a@test": "one", "b@test": "two"})
	if err := dists[0].Connect(context.Background(), "b@test"); !errors.Is(err, proc.NoConnection) {
		t.Errorf("Connect: %v", err)
	}
	pa, ca := inbox(nodes[0])
	pb, _ := inbox(nodes[1])
	in(nodes[0], pa, func(s *proc.Self) { s.Link(pb) })
	if m := next(t, ca); m != (proc.ExitMsg{From: pb, Reason: proc.NoConnection}) {
		t.Errorf("got %#v", m)
	}
}

// clusterWith starts nodes with cookies of their own.
func clusterWith(t *testing.T, cookies map[string]string) ([]*proc.Node, []*dist.Dist) {
	t.Helper()
	var mu sync.Mutex
	addrs := map[string]string{}
	resolve := func(node string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		a, ok := addrs[node]
		return a, ok
	}
	var nodes []*proc.Node
	var dists []*dist.Dist
	for _, name := range []string{"a@test", "b@test"} {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: cookies[name], Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(d.Stop)
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		nodes = append(nodes, n)
		dists = append(dists, d)
	}
	return nodes, dists
}

func TestUnknownNode(t *testing.T) {
	_, dists := cluster(t, "secret", "a@test")
	if err := dists[0].Connect(context.Background(), "nobody@test"); !errors.Is(err, proc.NoConnection) {
		t.Errorf("Connect: %v", err)
	}
}

// TestSimultaneous has two nodes connect to each other at once: one
// connection is kept, and what both sent arrives.
func TestSimultaneous(t *testing.T) {
	for range 20 {
		nodes, dists := cluster(t, "secret", "a@test", "b@test")
		a, b := nodes[0], nodes[1]
		pa, ca := inbox(a)
		pb, cb := inbox(b)
		var wg sync.WaitGroup
		wg.Go(func() { a.Send(pb, "from a") })
		wg.Go(func() { b.Send(pa, "from b") })
		wg.Wait()
		if m := next(t, cb); m != "from a" {
			t.Errorf("b got %#v", m)
		}
		if m := next(t, ca); m != "from b" {
			t.Errorf("a got %#v", m)
		}
		for i, d := range dists {
			if got := d.Nodes(); len(got) != 1 {
				t.Errorf("node %d is connected to %v", i, got)
			}
		}
		dists[0].Stop()
		dists[1].Stop()
	}
}

// unregistered is a type gob does not know in an interface.
type unregistered struct{ X int }

func TestEncodeError(t *testing.T) {
	nodes, _ := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	pb, cb := inbox(b)
	a.Send(pb, "before")
	a.Send(pb, unregistered{1})
	a.Send(pb, Ping{N: 2})
	for _, want := range []any{"before", Ping{N: 2}} {
		if m := next(t, cb); m != want {
			t.Errorf("got %#v, want %#v", m, want)
		}
	}
}

func TestUnnamed(t *testing.T) {
	if _, err := dist.Start(proc.NewNode(""), dist.Config{}); err == nil {
		t.Error("an unnamed node distributed")
	}
}

// counter is a genserver counting the casts it gets, and telling the
// count on a call.
type counter struct{}

func (counter) Init(proc.PID) (int, []gen.Effect, error) { return 0, nil, nil }

func (counter) HandleCall(n int, _ string, from genserver.From[int]) (int, []gen.Effect) {
	return n, gen.Do(from.Reply(n))
}

func (counter) HandleCast(n int, by int) (int, []gen.Effect) { return n + by, nil }

func TestRemoteName(t *testing.T) {
	nodes, _ := cluster(t, "secret", "a@test", "b@test")
	a, b := nodes[0], nodes[1]
	ctx := context.Background()
	if _, err := genserver.Start(ctx, b, counter{}, gen.WithName(gen.Local("counter"))); err != nil {
		t.Fatal(err)
	}
	remote := genserver.NewRef[string, int, int](gen.Remote{Node: "b@test", Name: "counter"})
	remote.Cast(a, 2)
	remote.Cast(a, 3)
	if got, err := remote.Call(ctx, a, "count"); err != nil || got != 5 {
		t.Errorf("call: %v, %v", got, err)
	}
	nobody := genserver.NewRef[string, int, int](gen.Remote{Node: "b@test", Name: "nobody"})
	if _, err := nobody.Call(ctx, a, "count"); !errors.Is(err, proc.NoProc) {
		t.Errorf("call to nobody: %v", err)
	}
	// A Remote name of the node itself is resolved there.
	here := genserver.NewRef[string, int, int](gen.Remote{Node: "b@test", Name: "counter"})
	if got, err := here.Call(ctx, b, "count"); err != nil || got != 5 {
		t.Errorf("call from b: %v, %v", got, err)
	}
}
