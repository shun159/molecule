package gen

import (
	"fmt"
	"runtime/debug"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// runner runs a Behaviour and performs its effects through an Env. It
// holds the meaning of the effects; the Env, what they act on.
//
// This file has its loop: a message in, the behaviour called, the effects
// performed. The effects are in effects.go, and what they keep track of
// in a file each: timers.go, monitors.go, asyncs.go, requests.go. The
// report of a behaviour terminating is in report.go.
type runner[S any] struct {
	env   Env
	b     Behaviour[S]
	state S

	timers   timers
	monitors monitors
	asyncs   asyncs
	requests requests

	stopping   bool
	stopReason error
	ackLater   bool // Init returned AckLater

	suspended bool
	deferred  []any // messages received while suspended, to handle once resumed

	last any // the message being handled, for the report on termination

	continues []any // Msgs of Continue effects, handled before the mailbox
}

func newRunner[S any](b Behaviour[S], env Env, o actor.StartOptions) *runner[S] {
	return &runner[S]{
		env:      env,
		b:        b,
		timers:   newTimers(env),
		monitors: newMonitors(env),
		asyncs:   newAsyncs(env, o.MaxAsync),
		requests: newRequests(env),
	}
}

// Init has the behaviour start, and acks its start, unless it acks later.
func (r *runner[S]) Init(args any) error {
	state, effs, err := r.init(args)
	if err != nil {
		return err
	}
	r.state = state
	r.apply(effs)
	if r.stopping {
		r.Abort()
		return r.stopReason
	}
	if !r.ackLater {
		r.env.Ack(nil)
	}
	return nil
}

func (r *runner[S]) init(args any) (state S, effs []actor.Effect, err error) {
	defer recovered(&err)
	if c, ok := any(r.b).(actor.Clocked); ok {
		b, ok := c.WithClock(r.env.Now).(Behaviour[S])
		if !ok {
			return state, nil, fmt.Errorf("gen: WithClock of %T returned another type of behaviour", r.b)
		}
		r.b = b
	}
	return r.b.Init(r.env.Self(), args)
}

func (r *runner[S]) State() any { return r.state }

func (r *runner[S]) Label() string { return labelOf(r.b) }

// Deliver handles msg from the mailbox, and what it leaves waiting.
func (r *runner[S]) Deliver(msg any) (bool, error) {
	if done, reason := r.receive(msg); done {
		return true, reason
	}
	return r.Flush()
}

// Flush handles what waits in the runner rather than the mailbox: the
// Msgs of the Continues of the last callback first, then, unless
// suspended, the messages deferred while it was.
func (r *runner[S]) Flush() (bool, error) {
	for {
		var done bool
		var reason error
		switch {
		case len(r.continues) > 0:
			// Still within the work of the last callback: no system
			// message or suspension comes in between.
			done, reason = r.step(ContinueMsg{Msg: popFront(&r.continues)})
		case !r.suspended && len(r.deferred) > 0:
			done, reason = r.receive(popFront(&r.deferred))
		default:
			return false, nil
		}
		if done {
			return true, reason
		}
	}
}

// popFront takes the first of q out, keeping no reference to it.
func popFront(q *[]any) any {
	x := (*q)[0]
	(*q)[0] = nil
	*q = (*q)[1:]
	return x
}

// receive handles a message from the mailbox: a system message, the exit
// of the parent, or a message for the behaviour, kept for later while
// suspended. It reports whether the behaviour terminated, and its exit
// reason if so.
func (r *runner[S]) receive(msg any) (bool, error) {
	if m, ok := msg.(sysMsg); ok {
		return r.system(m)
	}
	if e, ok := msg.(proc.ExitMsg); ok && r.isParent(e.From) {
		return r.parentExited(e)
	}
	if r.suspended {
		r.deferred = append(r.deferred, msg)
		return false, nil
	}
	if in, ok := r.translate(msg); ok {
		return r.step(in)
	}
	return false, nil
}

// isParent reports whether pid is the parent of the process; one started
// from outside a process has none.
func (r *runner[S]) isParent(pid proc.PID) bool {
	return !pid.IsZero() && pid == r.env.Parent()
}

// system handles a message of sys: terminating, or answering it.
func (r *runner[S]) system(m sysMsg) (bool, error) {
	if m.Req == sysTerminate {
		r.last = m
		return true, r.terminate(r.state, m.Reason)
	}
	var reply any
	switch m.Req {
	case sysGetState:
		reply = r.state
	case sysSuspend:
		r.suspended = true
	case sysResume:
		r.suspended = false
	}
	r.env.SendAlias(m.From.Tag, reply)
	return false, nil
}

// parentExited terminates the behaviour, on the exit of its parent, or
// has it handle the exit, as a ParentExiter.
func (r *runner[S]) parentExited(e proc.ExitMsg) (bool, error) {
	r.last = e
	pe, ok := any(r.b).(ParentExiter[S])
	if !ok {
		return true, r.terminate(r.state, e.Reason)
	}
	return r.performed(r.parentExit(pe, e.Reason))
}

// step has the behaviour handle in.
func (r *runner[S]) step(in Msg) (bool, error) {
	r.last = in
	return r.performed(r.handle(in))
}

// handle calls Handle, keeping the state it returns; a panic leaves the
// state as it was, for Terminate.
func (r *runner[S]) handle(in Msg) (effs []actor.Effect, err error) {
	defer recovered(&err)
	r.state, effs = r.b.Handle(r.state, in)
	return effs, nil
}

// parentExit calls ParentExit, as handle calls Handle.
func (r *runner[S]) parentExit(pe ParentExiter[S], reason error) (effs []actor.Effect, err error) {
	defer recovered(&err)
	r.state, effs = pe.ParentExit(r.state, reason)
	return effs, nil
}

// performed performs the effects a callback returned, or terminates the
// behaviour on its panic, err, or on a Stop among them. It reports
// whether the behaviour terminated, and its exit reason if so.
func (r *runner[S]) performed(effs []actor.Effect, err error) (bool, error) {
	if err != nil {
		return true, r.terminate(r.state, err)
	}
	r.apply(effs)
	if r.stopping {
		return true, r.terminate(r.state, r.stopReason)
	}
	return false, nil
}

// recovered, deferred, turns a panic of the callback it is deferred in
// into a PanicError in *err, as a process ending by one would have.
func recovered(err *error) {
	if v := recover(); v != nil {
		*err = &proc.PanicError{Value: v, Stack: debug.Stack()}
	}
}

// translate turns a received message into what the behaviour sees. It
// reports false for messages that are not for the behaviour, such as a
// timer that has since been cancelled.
func (r *runner[S]) translate(msg any) (Msg, bool) {
	switch m := msg.(type) {
	case actor.CallMsg, actor.CastMsg:
		return msg, true // boxed already: msg, not m, spares boxing it again
	case timeout:
		return r.timers.fired(m)
	case asyncDone:
		return r.asyncs.done(m)
	case answer:
		return r.requests.answered(m)
	case requestTimeout:
		return r.requests.timedOut(m)
	case responseMsg:
		return InfoMsg{Msg: m.Response}, true
	case downMsg:
		return InfoMsg{Msg: m.Down}, true
	case proc.DownMsg:
		return r.monitors.down(m), true
	default:
		return InfoMsg{Msg: msg}, true
	}
}

// terminate runs Terminate and returns the reason to exit with.
func (r *runner[S]) terminate(state S, reason error) (exit error) {
	defer r.Abort()
	defer recovered(&exit)
	if proc.IsAbnormal(reason) {
		r.report(state, reason)
	}
	effs := r.b.Terminate(state, reason)
	r.stopping = true // Stop has no further effect
	r.apply(effs)
	return reason
}

// Abort ends what the behaviour started that would outlive it: its
// timers, asyncs and requests.
func (r *runner[S]) Abort() {
	r.timers.abort()
	r.asyncs.abort()
	r.requests.abort()
}
