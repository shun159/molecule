package proc

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

type process struct {
	node *Node
	pid  PID
	mbox mailbox
	self Self // handed to the function of the process
	// A process is dead once dead is set, whether or not its goroutine
	// has returned: a killed process is dead at once, while its goroutine
	// only notices on its next Receive. reason is written before dead is
	// set, then the context cancelled, then done closed; all under mu.
	dead   atomic.Bool
	reason error
	done   chan struct{}

	// ctx is made when Self.Context is first called, and cancelled with
	// the exit reason on death. Guarded by mu.
	ctx    context.Context
	cancel context.CancelCauseFunc

	trapExit atomic.Bool

	// mu guards links, monitors and the transition to dead, so a link or
	// a monitor is either established with a live process or reported as
	// NoProc, never lost.
	mu    sync.Mutex
	links map[PID]struct{}
	// monitors are the watchers of p; monitoring are the processes p
	// watches. A DOWN message is only queued while its Ref is still in
	// the watcher's monitoring, checked under the watcher's lock.
	monitors   map[Ref]watcher
	monitoring map[Ref]PID

	// name is the registered name, guarded by node.regMu.
	name string

	// parent is the process that spawned p, if any. ack, when set, is the
	// alias InitAck reports to for a waiting Start or StartLink. Both are
	// set before p runs; ack is then only touched by p's own goroutine.
	parent PID
	ack    Ref
}

func newProcess(n *Node, pid PID) *process {
	p := &process{
		node: n,
		pid:  pid,
		mbox: mailbox{notify: make(chan struct{}, 1)},
		done: make(chan struct{}),
	}
	p.self.p = p
	return p
}

// exitReason returns the exit reason of p, nil while it is alive.
func (p *process) exitReason() error {
	if !p.dead.Load() {
		return nil
	}
	return p.reason
}

func (p *process) run(fn func(*Self) error) {
	if p.dead.Load() {
		return // killed before it started
	}
	// runtime.Goexit unwinds through invoke without returning, so the exit
	// must happen in a deferred call.
	reason := ErrGoexit
	defer func() {
		// Only a process ending by itself is reported: one that was killed
		// or died from an exit signal ran no code of its own to fail, as in
		// Erlang. The report comes before the links and monitors learn of
		// the death.
		if !p.dead.Load() && IsAbnormal(reason) {
			p.crashReport(fn, reason)
		}
		p.die(reason)
	}()
	reason = invoke(fn, &p.self)
}

// invoke runs fn and converts the way it ended into an exit reason.
func invoke(fn func(*Self) error, self *Self) (reason error) {
	defer func() {
		if r := recover(); r != nil {
			reason = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	if err := fn(self); err != nil {
		return err
	}
	return Normal
}

// die makes p dead with reason, propagates it to the linked processes and
// notifies the monitoring ones. Only the first call has an effect.
func (p *process) die(reason error) {
	p.mu.Lock()
	if p.dead.Load() {
		p.mu.Unlock()
		return
	}
	p.reason = reason
	p.dead.Store(true)
	if p.cancel != nil {
		p.cancel(reason)
	}
	close(p.done) // last: once it is closed, the context is cancelled too
	links, monitors, monitoring := p.links, p.monitors, p.monitoring
	p.links, p.monitors, p.monitoring = nil, nil, nil
	p.mu.Unlock()

	p.node.procs.del(p.pid.id)
	// The name goes before anyone is told, so a supervisor reacting to the
	// death can register a replacement under the same name at once.
	p.node.unregisterDead(p)
	for to := range links {
		p.node.sendExit(p.pid, to, reason, true)
	}
	for ref, w := range monitors {
		w.notify(p.node, ref, p.pid, reason)
	}
	for ref, target := range monitoring {
		if t := p.node.lookup(target); t != nil {
			t.mu.Lock()
			delete(t.monitors, ref)
			t.mu.Unlock()
		}
	}
}

// signalExit handles an exit signal sent to p by from. Signals that kill p
// take effect immediately; the rest become ExitMsg in the mailbox, so they
// stay ordered with the messages from the same sender.
func (p *process) signalExit(from PID, reason error, viaLink bool) {
	if reason == nil {
		reason = Normal
	}
	if viaLink {
		p.mu.Lock()
		_, linked := p.links[from]
		delete(p.links, from)
		p.mu.Unlock()
		if !linked {
			return // unlinked in the meantime
		}
	}
	switch {
	case !viaLink && errors.Is(reason, Kill):
		p.die(Killed)
	case p.trapExit.Load():
		p.mbox.push(ExitMsg{From: from, Reason: reason})
	case errors.Is(reason, Normal):
		if from == p.pid {
			p.die(Normal)
		}
	default:
		p.die(reason)
	}
}

// The tables of a process are made when first needed: most processes link
// and monitor little, if at all. They are guarded by mu, and are not
// written once the process is dead.

func (p *process) link(pid PID) {
	if p.links == nil {
		p.links = make(map[PID]struct{})
	}
	p.links[pid] = struct{}{}
}

// watchedBy records w as a watcher of p.
func (p *process) watchedBy(ref Ref, w watcher) {
	if p.monitors == nil {
		p.monitors = make(map[Ref]watcher)
	}
	p.monitors[ref] = w
}

// watch records that p monitors target.
func (p *process) watch(ref Ref, target PID) {
	if p.monitoring == nil {
		p.monitoring = make(map[Ref]PID)
	}
	p.monitoring[ref] = target
}

// lockPair locks two distinct processes in a fixed order to avoid
// deadlocks between concurrent Link calls.
func lockPair(a, b *process) (unlock func()) {
	if a.pid.id > b.pid.id {
		a, b = b, a
	}
	a.mu.Lock()
	b.mu.Lock()
	return func() {
		b.mu.Unlock()
		a.mu.Unlock()
	}
}

// Self is the handle a process has on itself. It must only be used by the
// goroutine running the process.
type Self struct {
	p *process
}

// PID returns the PID of the process.
func (s *Self) PID() PID { return s.p.pid }

// Node returns the node the process runs on.
func (s *Self) Node() *Node { return s.p.node }

// Context returns a context that is cancelled when the process dies, with
// the exit reason as its cause. Pass it to blocking I/O done on behalf of
// the process.
func (s *Self) Context() context.Context {
	p := s.p
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx == nil {
		p.ctx, p.cancel = context.WithCancelCause(context.Background())
		if p.dead.Load() {
			p.cancel(p.reason)
		}
	}
	return p.ctx
}

// Done returns a channel closed when the process dies. It is lighter
// than Context, for code that only waits on the death.
func (s *Self) Done() <-chan struct{} { return s.p.done }

// ExitReason returns the exit reason of the process once it is dead, nil
// while it is alive.
func (s *Self) ExitReason() error { return s.p.exitReason() }

// Parent returns the process that spawned this one, or the zero PID for a
// process started from outside any process.
func (s *Self) Parent() PID { return s.p.parent }

// Spawn starts fn in a new process on the same node.
func (s *Self) Spawn(fn func(*Self) error) PID {
	child := s.p.node.register()
	child.parent = s.p.pid
	go child.run(fn)
	return child.pid
}

// SpawnLink starts fn in a new process linked to the caller. The link is
// in place before fn runs, so even an immediate crash is propagated.
func (s *Self) SpawnLink(fn func(*Self) error) PID {
	child := s.newLinked()
	go child.run(fn)
	return child.pid
}

// newLinked registers a child of s linked to it, without starting it.
func (s *Self) newLinked() *process {
	parent := s.p
	child := parent.node.register()
	child.parent = parent.pid
	child.link(parent.pid)

	parent.mu.Lock()
	alive := !parent.dead.Load()
	if alive {
		parent.link(child.pid)
	}
	parent.mu.Unlock()
	if !alive {
		child.signalExit(parent.pid, parent.exitReason(), true)
	}
	return child
}

// Send delivers msg to to. See Node.Send.
//
// A dead process sends nothing: once killed, the code its goroutine still
// runs reaches no one.
func (s *Self) Send(to PID, msg any) {
	if !s.p.dead.Load() {
		s.p.node.Send(to, msg)
	}
}

// Link creates a bidirectional link with to: when either process dies, the
// other gets an exit signal. Linking to a process that does not exist
// delivers an exit signal with reason NoProc to the caller.
func (s *Self) Link(to PID) {
	p := s.p
	if to == p.pid {
		return
	}
	t := p.node.lookup(to)
	if t == nil {
		reason := NoProc
		if !p.node.isLocal(to) {
			reason = NoConnection // TODO(dist): link to remote processes.
		}
		p.signalExit(to, reason, false)
		return
	}

	unlock := lockPair(p, t)
	switch {
	case p.dead.Load():
		unlock()
	case t.dead.Load():
		unlock()
		p.signalExit(to, NoProc, false)
	default:
		p.link(to)
		t.link(p.pid)
		unlock()
	}
}

// Unlink removes the link with to, if any. No exit signal from that link is
// delivered afterwards, though an ExitMsg already in the mailbox stays there.
func (s *Self) Unlink(to PID) {
	p := s.p
	t := p.node.lookup(to)
	if t == nil || t == p {
		p.mu.Lock()
		delete(p.links, to)
		p.mu.Unlock()
		return
	}
	unlock := lockPair(p, t)
	delete(p.links, to)
	delete(t.links, p.pid)
	unlock()
}

// TrapExit sets whether exit signals are turned into ExitMsg instead of
// terminating the process, and returns the previous setting.
func (s *Self) TrapExit(on bool) bool { return s.p.trapExit.Swap(on) }

// Exit sends an exit signal with reason to to. Kill terminates to even if
// it traps exits. Normal is ignored by a process that does not trap exits,
// unless it is the caller itself.
//
// A dead process sends no exit signal.
func (s *Self) Exit(to PID, reason error) {
	if !s.p.dead.Load() {
		s.p.node.sendExit(s.p.pid, to, reason, false)
	}
}

// Receive returns the next message in the mailbox, blocking until one
// arrives. It returns ctx.Err() if ctx is done first, and the exit reason
// if the process dies meanwhile.
func (s *Self) Receive(ctx context.Context) (any, error) {
	p := s.p
	for {
		if p.dead.Load() {
			return nil, p.reason
		}
		if msg, ok := p.mbox.pop(); ok {
			return msg, nil
		}
		select {
		case <-p.mbox.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
		}
	}
}
