package proc

import (
	"context"
	"runtime/debug"
)

type process struct {
	node *Node
	pid  PID
	mbox *mailbox
	// ctx is cancelled with the exit reason as its cause once the process
	// is dead. Whether it is dead is decided by ctx alone, not by whether
	// the goroutine has returned.
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func newProcess(n *Node, pid PID) *process {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &process{node: n, pid: pid, mbox: newMailbox(), ctx: ctx, cancel: cancel}
}

func (p *process) run(fn func(*Self) error) {
	// runtime.Goexit unwinds through invoke without returning, so the exit
	// must happen in a deferred call.
	reason := ErrGoexit
	defer func() { p.exit(reason) }()
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

func (p *process) exit(reason error) {
	p.node.procs.Delete(p.pid.id)
	p.cancel(reason)
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

// Spawn starts fn in a new process on the same node.
func (s *Self) Spawn(fn func(*Self) error) PID { return s.p.node.Spawn(fn) }

// Send delivers msg to to. See Node.Send.
func (s *Self) Send(to PID, msg any) { s.p.node.Send(to, msg) }

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
