package proc

import "context"

// DownMsg is delivered to a watcher when the monitored process dies, like
// {'DOWN', Ref, process, PID, Reason} in Erlang.
type DownMsg struct {
	Ref    Ref
	PID    PID
	Reason error
}

// watcher is who is told when a monitored process dies: a process,
// through a DownMsg; a Watch context, by cancelling it; or a MonitorAlias,
// through its channel.
type watcher struct {
	pid    PID
	cancel context.CancelCauseFunc
	alias  bool
}

func (w watcher) notify(n *Node, ref Ref, target PID, reason error) {
	switch {
	case w.cancel != nil:
		w.cancel(reason)
		return
	case w.alias:
		n.aliasDown(ref, reason)
		return
	}
	if p := n.lookup(w.pid); p != nil {
		p.down(ref, target, reason)
	}
	// TODO(dist): notify remote watchers.
}

// down queues a DownMsg to p, the watcher, unless the monitor has been
// removed meanwhile. The check and the push are done under p.mu so a
// Demonitor that has returned never sees a DOWN for its Ref afterwards.
func (p *process) down(ref Ref, target PID, reason error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.monitoring[ref]; !ok {
		return
	}
	delete(p.monitoring, ref)
	p.mbox.push(DownMsg{Ref: ref, PID: target, Reason: reason})
}

// Monitor starts watching target. When target dies, a DownMsg carrying the
// returned Ref is queued to the caller; unlike a link, the caller itself is
// never affected. Monitoring a process that does not exist queues a DownMsg
// with reason NoProc at once. Each call creates an independent monitor.
func (s *Self) Monitor(target PID) Ref {
	p := s.p
	ref := p.node.MakeRef()
	if target == p.pid {
		return ref // allowed, but can never fire
	}

	t := p.node.lookup(target)
	if t == nil {
		reason := NoProc
		if !p.node.isLocal(target) {
			reason = NoConnection // TODO(dist): monitor remote processes.
		}
		p.mbox.push(DownMsg{Ref: ref, PID: target, Reason: reason})
		return ref
	}

	unlock := lockPair(p, t)
	defer unlock()
	switch {
	case p.ctx.Err() != nil:
	case t.ctx.Err() != nil:
		p.mbox.push(DownMsg{Ref: ref, PID: target, Reason: NoProc})
	default:
		p.monitoring[ref] = target
		t.monitors[ref] = watcher{pid: p.pid}
	}
	return ref
}

// Demonitor stops the monitor ref and removes its DownMsg from the mailbox
// if one was already queued, like erlang:demonitor(Ref, [flush, info]).
// It reports whether the monitor was still active, i.e. the target had not
// been reported down.
func (s *Self) Demonitor(ref Ref) bool {
	p := s.p
	p.mu.Lock()
	target, active := p.monitoring[ref]
	delete(p.monitoring, ref)
	p.mu.Unlock()

	if active {
		if t := p.node.lookup(target); t != nil {
			t.mu.Lock()
			delete(t.monitors, ref)
			t.mu.Unlock()
		}
		return true
	}
	p.mbox.remove(func(msg any) bool {
		d, ok := msg.(DownMsg)
		return ok && d.Ref == ref
	})
	return false
}

// Watch returns a context that is cancelled when pid dies, with its exit
// reason as the cause. It is how code that is not a process, or a process
// that must wait on something besides its mailbox, observes a death:
//
//	ctx, stop := n.Watch(parent, pid)
//	defer stop()
//	select {
//	case r := <-reply:
//	case <-ctx.Done():
//		return context.Cause(ctx) // exit reason, or parent's cause
//	}
//
// The context is also cancelled when parent is. If pid does not exist the
// context is already cancelled with NoProc. As with context.WithCancel,
// stop must be called to release the watch once it is no longer needed;
// after stop, the cause is context.Canceled unless pid had died first.
func (n *Node) Watch(parent context.Context, pid PID) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	stop := func() { cancel(context.Canceled) }

	t := n.lookup(pid)
	if t == nil {
		reason := NoProc
		if !n.isLocal(pid) {
			reason = NoConnection // TODO(dist): watch remote processes.
		}
		cancel(reason)
		return ctx, stop
	}

	ref := n.MakeRef()
	t.mu.Lock()
	if t.ctx.Err() != nil {
		t.mu.Unlock()
		cancel(NoProc)
		return ctx, stop
	}
	t.monitors[ref] = watcher{cancel: cancel}
	t.mu.Unlock()

	return ctx, func() {
		t.mu.Lock()
		delete(t.monitors, ref)
		t.mu.Unlock()
		stop()
	}
}
