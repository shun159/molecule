package proc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeNet connects nodes in memory: what a node sends another goes, in
// order, through a queue per direction to the Remote of the other. A cut
// link loses what it carries and reports the nodes down to each other.
type fakeNet struct {
	mu      sync.Mutex
	remotes map[string]Remote
	queues  map[[2]string]chan func()
	cut     map[[2]string]bool
}

func newFakeNet(t *testing.T, names ...string) (*fakeNet, []*Node) {
	f := &fakeNet{remotes: map[string]Remote{}, queues: map[[2]string]chan func(){}, cut: map[[2]string]bool{}}
	var nodes []*Node
	for _, name := range names {
		n := NewNode(name)
		f.remotes[name] = n.Distribute(fakeDist{f, name})
		nodes = append(nodes, n)
	}
	t.Cleanup(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, q := range f.queues {
			close(q)
		}
	})
	return f, nodes
}

// do runs fn on the Remote of node to, after what from sent it before.
func (f *fakeNet) do(from, to string, fn func(Remote)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := [2]string{from, to}
	if f.cut[key] {
		go f.remotes[from].NodeDown(to)
		return
	}
	r, ok := f.remotes[to]
	if !ok {
		go f.remotes[from].NodeDown(to)
		return
	}
	q := f.queues[key]
	if q == nil {
		q = make(chan func(), 1024)
		f.queues[key] = q
		go func() {
			for fn := range q {
				fn()
			}
		}()
	}
	q <- func() { fn(r) }
}

// disconnect cuts a and b apart.
func (f *fakeNet) disconnect(a, b string) {
	f.mu.Lock()
	f.cut[[2]string{a, b}], f.cut[[2]string{b, a}] = true, true
	ra, rb := f.remotes[a], f.remotes[b]
	f.mu.Unlock()
	ra.NodeDown(b)
	rb.NodeDown(a)
}

type fakeDist struct {
	f    *fakeNet
	from string
}

func (d fakeDist) Send(to PID, msg any) {
	d.f.do(d.from, to.node, func(r Remote) { r.Send(to, msg) })
}
func (d fakeDist) SendName(node, name string, msg any) {
	d.f.do(d.from, node, func(r Remote) { r.SendName(name, msg) })
}
func (d fakeDist) SendAlias(ref Ref, msg any) {
	d.f.do(d.from, ref.node, func(r Remote) { r.SendAlias(ref, msg) })
}
func (d fakeDist) Exit(from, to PID, reason error, viaLink bool) {
	d.f.do(d.from, to.node, func(r Remote) { r.Exit(from, to, reason, viaLink) })
}
func (d fakeDist) Link(from, to PID) {
	d.f.do(d.from, to.node, func(r Remote) { r.Link(from, to) })
}
func (d fakeDist) Unlink(from, to PID) {
	d.f.do(d.from, to.node, func(r Remote) { r.Unlink(from, to) })
}
func (d fakeDist) Monitor(ref Ref, target PID) {
	d.f.do(d.from, target.node, func(r Remote) { r.Monitor(ref, target) })
}
func (d fakeDist) Demonitor(ref Ref, target PID) {
	d.f.do(d.from, target.node, func(r Remote) { r.Demonitor(ref, target) })
}
func (d fakeDist) Down(ref Ref, target PID, reason error) {
	d.f.do(d.from, ref.node, func(r Remote) { r.Down(ref, target, reason) })
}

// inbox is a process handing what it receives to a channel, and running
// the functions sent to it.
func inbox(n *Node) (PID, <-chan any) {
	ch := make(chan any, 16)
	pid := n.Spawn(func(s *Self) error {
		s.TrapExit(true)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if fn, ok := msg.(func(*Self)); ok {
				fn(s)
				continue
			}
			ch <- msg
		}
	})
	return pid, ch
}

func next(t *testing.T, ch <-chan any) any {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("nothing received")
		return nil
	}
}

func nothing(t *testing.T, ch <-chan any) {
	t.Helper()
	select {
	case m := <-ch:
		t.Errorf("unexpected %#v", m)
	case <-time.After(30 * time.Millisecond):
	}
}

// in runs fn in the process pid, an inbox, and waits for it.
func in(n *Node, pid PID, fn func(*Self)) {
	done := make(chan struct{})
	n.Send(pid, func(s *Self) {
		fn(s)
		close(done)
	})
	<-done
}

func TestRemoteSend(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, cb := inbox(b)
	for i := range 10 {
		a.Send(pb, i)
	}
	for i := range 10 {
		if m := next(t, cb); m != i {
			t.Fatalf("got %v, want %d", m, i)
		}
	}
	b.Register("echo", pb)
	a.SendName("b", "echo", "by name")
	if m := next(t, cb); m != "by name" {
		t.Errorf("got %v", m)
	}
	// A stale PID of b, of a previous incarnation, reaches no one.
	stale := pb
	stale.creation++
	a.Send(stale, "lost")
	nothing(t, cb)
	nothing(t, ca)
	_ = pa
}

func TestRemoteLink(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	boom := errors.New("boom")
	pb := b.Spawn(func(s *Self) error {
		_, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		return boom
	})
	in(a, pa, func(s *Self) { s.Link(pb) })
	// The link is made at b before the message that ends pb.
	a.Send(pb, "go")
	if m := next(t, ca); m != (ExitMsg{From: pb, Reason: boom}) {
		t.Errorf("got %#v", m)
	}

	// The other way: pa dies, a process of b linked to it gets the signal.
	pb2, cb2 := inbox(b)
	victim := a.Spawn(func(s *Self) error {
		s.Link(pb2)
		_, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		return boom
	})
	a.Send(victim, "go")
	if m := next(t, cb2); m != (ExitMsg{From: victim, Reason: boom}) {
		t.Errorf("got %#v", m)
	}

	// Linking a process that does not exist fails with NoProc.
	gone := b.NewPID()
	in(a, pa, func(s *Self) { s.Link(gone) })
	if m := next(t, ca); m != (ExitMsg{From: gone, Reason: NoProc}) {
		t.Errorf("got %#v", m)
	}
}

func TestRemoteUnlink(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, _ := inbox(b)
	in(a, pa, func(s *Self) {
		s.Link(pb)
		s.Unlink(pb)
	})
	a.Send(pb, func(s *Self) {}) // after the unlink, at b
	b.lookup(pb).die(errors.New("boom"))
	nothing(t, ca)
	if p := b.lookup(pb); p != nil {
		t.Error("pb still alive")
	}
}

func TestRemoteExitKill(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pa, _ := inbox(a)
	pb, _ := inbox(b) // traps exits
	ctx, stop := a.Watch(context.Background(), pb)
	defer stop()
	in(a, pa, func(s *Self) { s.Exit(pb, Kill) })
	<-ctx.Done()
	if err := context.Cause(ctx); err != Killed {
		t.Errorf("exit reason = %v, want killed", err)
	}
}

func TestRemoteMonitor(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, _ := inbox(b)
	boom := errors.New("boom")

	var ref Ref
	in(a, pa, func(s *Self) { ref = s.Monitor(pb) })
	a.Send(pb, func(*Self) {}) // the monitor is in place at b after this
	b.Send(pb, func(*Self) {})
	eventually(t, func() bool { return len(watchers(b, pb)) == 1 })
	b.lookup(pb).die(boom)
	if m := next(t, ca); m != (DownMsg{Ref: ref, PID: pb, Reason: boom}) {
		t.Errorf("got %#v", m)
	}

	// Demonitored, nothing arrives, and the target forgets the monitor.
	pb2, _ := inbox(b)
	in(a, pa, func(s *Self) {
		ref := s.Monitor(pb2)
		s.Demonitor(ref)
	})
	eventually(t, func() bool { return len(watchers(b, pb2)) == 0 })
	b.lookup(pb2).die(boom)
	nothing(t, ca)

	// A process that does not exist is down at once, with NoProc.
	gone := b.NewPID()
	in(a, pa, func(s *Self) { ref = s.Monitor(gone) })
	if m := next(t, ca); m != (DownMsg{Ref: ref, PID: gone, Reason: NoProc}) {
		t.Errorf("got %#v", m)
	}

	// A watcher dying ends its monitors at the target.
	pb3, _ := inbox(b)
	watcherProc, _ := inbox(a)
	in(a, watcherProc, func(s *Self) { s.Monitor(pb3) })
	eventually(t, func() bool { return len(watchers(b, pb3)) == 1 })
	a.lookup(watcherProc).die(boom)
	eventually(t, func() bool { return len(watchers(b, pb3)) == 0 })
}

func TestRemoteAlias(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pb, _ := inbox(b)

	// A reply comes back to the alias, as for a call.
	al := a.MonitorAlias(pb)
	a.Send(pb, func(s *Self) { s.Node().SendAlias(al.Ref, "reply") })
	if m := <-al.C; m.Down || m.Msg != "reply" {
		t.Errorf("got %#v", m)
	}
	al.Release()
	eventually(t, func() bool { return len(watchers(b, pb)) == 0 })

	// The death of the target comes instead, if first.
	al = a.MonitorAlias(pb)
	defer al.Release()
	eventually(t, func() bool { return len(watchers(b, pb)) == 1 })
	boom := errors.New("boom")
	b.lookup(pb).die(boom)
	if m := <-al.C; !m.Down || m.Reason != boom {
		t.Errorf("got %#v", m)
	}
}

func TestRemoteWatch(t *testing.T) {
	_, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pb, _ := inbox(b)
	ctx, stop := a.Watch(context.Background(), pb)
	defer stop()
	eventually(t, func() bool { return len(watchers(b, pb)) == 1 })
	boom := errors.New("boom")
	b.lookup(pb).die(boom)
	<-ctx.Done()
	if err := context.Cause(ctx); err != boom {
		t.Errorf("cause = %v", err)
	}
}

func TestNodeDown(t *testing.T) {
	f, nodes := newFakeNet(t, "a", "b")
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, cb := inbox(b)
	var ref Ref
	in(a, pa, func(s *Self) {
		s.Link(pb)
		ref = s.Monitor(pb)
	})
	ctx, stop := a.Watch(context.Background(), pb)
	defer stop()
	in(b, pb, func(s *Self) { s.Monitor(pa) })
	eventually(t, func() bool { return len(watchers(b, pb)) == 2 && len(watchers(a, pa)) == 1 })

	f.disconnect("a", "b")
	got := map[any]bool{next(t, ca): true, next(t, ca): true}
	if !got[ExitMsg{From: pb, Reason: NoConnection}] || !got[DownMsg{Ref: ref, PID: pb, Reason: NoConnection}] {
		t.Errorf("at a: %v", got)
	}
	<-ctx.Done()
	if err := context.Cause(ctx); err != NoConnection {
		t.Errorf("watch cause = %v", err)
	}
	var exited, down bool
	for range 2 {
		switch m := next(t, cb).(type) {
		case ExitMsg:
			exited = m == ExitMsg{From: pa, Reason: NoConnection}
		case DownMsg:
			down = m.PID == pa && m.Reason == NoConnection
		}
	}
	if !exited || !down {
		t.Errorf("at b: exit %v, down %v", exited, down)
	}
	if w := watchers(a, pa); len(w) != 0 {
		t.Errorf("a keeps monitors of b: %v", w)
	}

	// Cut apart, links and monitors fail at once.
	in(a, pa, func(s *Self) {
		s.Link(pb)
		ref = s.Monitor(pb)
	})
	got = map[any]bool{next(t, ca): true, next(t, ca): true}
	if !got[ExitMsg{From: pb, Reason: NoConnection}] || !got[DownMsg{Ref: ref, PID: pb, Reason: NoConnection}] {
		t.Errorf("after the cut: %v", got)
	}
}

func TestIDMarshal(t *testing.T) {
	n := NewNode("node@host")
	pid := n.Spawn(func(*Self) error { return nil })
	b, _ := pid.MarshalBinary()
	var got PID
	if err := got.UnmarshalBinary(b); err != nil || got != pid {
		t.Errorf("PID round trip: %v, %v", got, err)
	}
	ref := n.MakeRef()
	b, _ = ref.MarshalBinary()
	var gotRef Ref
	if err := gotRef.UnmarshalBinary(b); err != nil || gotRef != ref {
		t.Errorf("Ref round trip: %v, %v", gotRef, err)
	}
	if err := got.UnmarshalBinary(b[:len(b)-1]); err == nil {
		t.Error("short input accepted")
	}
}

// watchers returns the monitors of the local process pid.
func watchers(n *Node, pid PID) []Ref {
	p := n.lookup(pid)
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var refs []Ref
	for ref := range p.monitors {
		refs = append(refs, ref)
	}
	return refs
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition never met")
		}
		time.Sleep(time.Millisecond)
	}
}
