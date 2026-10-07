package proc

import "context"

// StartLink starts fn in a new process linked to the caller and waits until
// it calls InitAck, like proc_lib:start_link. It returns the PID once fn
// acknowledges with nil, or the error fn acknowledged with. If fn exits
// before acknowledging, StartLink returns its exit reason; through the link
// that also terminates a caller that does not trap exits, as in Erlang. A
// caller that traps exits gets no ExitMsg for a child that failed to start.
//
// If ctx is done first, the child is unlinked and killed, and ctx.Err() is
// returned.
func (s *Self) StartLink(ctx context.Context, fn func(*Self) error) (PID, error) {
	return s.p.node.start(ctx, s, s.newLinked(), fn)
}

// Start starts fn in a new process and waits until it calls InitAck, like
// proc_lib:start. Unlike StartLink it may be called from outside any
// process, e.g. to start the top supervisor from main.
func (n *Node) Start(ctx context.Context, fn func(*Self) error) (PID, error) {
	return n.start(ctx, nil, n.register(), fn)
}

// InitAck reports the outcome of initialization to the Start or StartLink
// waiting for this process: nil for success, or the error to return to it.
// The process keeps running either way; after a failure it is expected to
// exit. Only the first call has an effect, and none for a process that was
// not started by Start or StartLink.
func (s *Self) InitAck(err error) {
	if !s.p.ack.IsZero() {
		s.p.node.SendAlias(s.p.ack, err)
		s.p.ack = Ref{}
	}
}

// start runs the registered, not yet started child and waits for its
// acknowledgement. caller is nil when called from outside a process.
func (n *Node) start(ctx context.Context, caller *Self, child *process, fn func(*Self) error) (PID, error) {
	// The ack and the death of the child arrive through one alias, set up
	// before the child runs: an immediate death reports its real reason,
	// and an ack sent before dying comes first, as in Erlang.
	a := n.MonitorAlias(child.pid)
	defer a.Release()
	child.ack = a.Ref
	go child.run(fn)
	return awaitStart(ctx, caller, child, a.C)
}

// awaitStart waits for the child to acknowledge or die, for ctx, or for
// the caller to die.
func awaitStart(ctx context.Context, caller *Self, child *process, c <-chan AliasMsg) (PID, error) {
	var callerDone <-chan struct{}
	if caller != nil {
		callerDone = caller.p.done
	}

	select {
	case m := <-c:
		if m.Down {
			caller.flushExit(child.pid)
			return PID{}, m.Reason
		}
		if err, _ := m.Msg.(error); err != nil {
			return PID{}, err
		}
		return child.pid, nil
	case <-ctx.Done():
		if caller != nil {
			caller.Unlink(child.pid)
		}
		child.signalExit(PID{}, Kill, false)
		caller.flushExit(child.pid)
		return PID{}, ctx.Err()
	case <-callerDone:
		return PID{}, caller.p.exitReason()
	}
}

// flushExit removes the ExitMsg from pid, if any. The link notification
// that queues it happens before the monitors, so it is in the mailbox by
// the time a monitor of pid fires. A nil s is allowed.
func (s *Self) flushExit(pid PID) {
	if s == nil {
		return
	}
	s.p.mbox.remove(func(msg any) bool {
		e, ok := msg.(ExitMsg)
		return ok && e.From == pid
	})
}
