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
	mbox *mailbox
	// ctx is cancelled with the exit reason as its cause once the process
	// is dead. Whether it is dead is decided by ctx alone, not by whether
	// the goroutine has returned: a killed process is dead at once, while
	// its goroutine only notices on its next Receive.
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
	ctx, cancel := context.WithCancelCause(context.Background())
	return &process{
		node:   n,
		pid:    pid,
		mbox:   newMailbox(),
		ctx:    ctx,
		cancel: cancel,
		links:  make(map[PID]struct{}),

		monitors:   make(map[Ref]watcher),
		monitoring: make(map[Ref]PID),
	}
}

func (p *process) run(fn func(*Self) error) {
	if p.ctx.Err() != nil {
		return // killed before it started
	}
	// runtime.Goexit unwinds through invoke without returning, so the exit
	// must happen in a deferred call.
	reason := ErrGoexit
	defer func() { p.die(reason) }()
	reason = invoke(fn, &Self{p: p})
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
	if p.ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.cancel(reason)
	links, monitors, monitoring := p.links, p.monitors, p.monitoring
	p.links, p.monitors, p.monitoring = nil, nil, nil
	p.mu.Unlock()

	p.node.procs.Delete(p.pid.id)
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
func (s *Self) Context() context.Context { return s.p.ctx }

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
	child.links[parent.pid] = struct{}{}

	parent.mu.Lock()
	alive := parent.ctx.Err() == nil
	if alive {
		parent.links[child.pid] = struct{}{}
	}
	parent.mu.Unlock()
	if !alive {
		child.signalExit(parent.pid, context.Cause(parent.ctx), true)
	}
	return child
}

// Send delivers msg to to. See Node.Send.
func (s *Self) Send(to PID, msg any) { s.p.node.Send(to, msg) }

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
	case p.ctx.Err() != nil:
		unlock()
	case t.ctx.Err() != nil:
		unlock()
		p.signalExit(to, NoProc, false)
	default:
		p.links[to] = struct{}{}
		t.links[p.pid] = struct{}{}
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
func (s *Self) Exit(to PID, reason error) {
	s.p.node.sendExit(s.p.pid, to, reason, false)
}

// Receive returns the next message in the mailbox, blocking until one
// arrives. It returns ctx.Err() if ctx is done first, and the exit reason
// if the process dies meanwhile.
func (s *Self) Receive(ctx context.Context) (any, error) {
	p := s.p
	for {
		if p.ctx.Err() != nil {
			return nil, context.Cause(p.ctx)
		}
		if msg, ok := p.mbox.pop(); ok {
			return msg, nil
		}
		select {
		case <-p.mbox.notify:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.ctx.Done():
		}
	}
}
