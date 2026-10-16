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
	deferred  []any

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
	defer func() {
		if v := recover(); v != nil {
			err = &proc.PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
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

func (r *runner[S]) Deliver(msg any) (bool, error) {
	if done, reason := r.receive(msg); done {
		return true, reason
	}
	return r.Flush()
}

func (r *runner[S]) Flush() (bool, error) {
	for {
		switch {
		case len(r.continues) > 0:
			// Still within the work of the last callback: no system
			// message or suspension comes in between.
			c := r.continues[0]
			r.continues[0] = nil
			r.continues = r.continues[1:]
			if done, reason := r.step(ContinueMsg{Msg: c}); done {
				return true, reason
			}
		case !r.suspended && len(r.deferred) > 0:
			msg := r.deferred[0]
			r.deferred[0] = nil
			r.deferred = r.deferred[1:]
			if done, reason := r.receive(msg); done {
				return true, reason
			}
		default:
			return false, nil
		}
	}
}

// receive handles a message from the mailbox.
func (r *runner[S]) receive(msg any) (bool, error) {
	if m, ok := msg.(sysMsg); ok {
		if m.Req == sysTerminate {
			r.last = m
			return true, r.terminate(r.state, m.Reason)
		}
		r.system(m)
		return false, nil
	}
	if e, ok := msg.(proc.ExitMsg); ok && r.isParent(e.From) {
		r.last = e
		if pe, ok := any(r.b).(ParentExiter[S]); ok {
			return r.parentExit(pe, e.Reason)
		}
		return true, r.terminate(r.state, e.Reason)
	}
	if r.suspended {
		r.deferred = append(r.deferred, msg)
		return false, nil
	}
	in, ok := r.translate(msg)
	if !ok {
		return false, nil
	}
	return r.step(in)
}

// isParent reports whether pid is the parent of the process; one started
// from outside a process has none.
func (r *runner[S]) isParent(pid proc.PID) bool {
	return !pid.IsZero() && pid == r.env.Parent()
}

// step handles one message and performs the effects. It reports whether
// the behaviour has terminated, and its exit reason if so.
func (r *runner[S]) step(in Msg) (bool, error) {
	r.last = in
	state, effs, err := r.handle(in)
	if err != nil {
		return true, r.terminate(r.state, err)
	}
	r.state = state
	r.apply(effs)
	if r.stopping {
		return true, r.terminate(r.state, r.stopReason)
	}
	return false, nil
}

// parentExit has the behaviour handle the exit of its parent.
func (r *runner[S]) parentExit(pe ParentExiter[S], reason error) (bool, error) {
	state, effs, err := func() (state S, effs []actor.Effect, err error) {
		defer func() {
			if v := recover(); v != nil {
				err = &proc.PanicError{Value: v, Stack: debug.Stack()}
			}
		}()
		state, effs = pe.ParentExit(r.state, reason)
		return state, effs, nil
	}()
	if err != nil {
		return true, r.terminate(r.state, err)
	}
	r.state = state
	r.apply(effs)
	if r.stopping {
		return true, r.terminate(r.state, r.stopReason)
	}
	return false, nil
}

func (r *runner[S]) handle(in Msg) (state S, effs []actor.Effect, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &proc.PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	state, effs = r.b.Handle(r.state, in)
	return state, effs, nil
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
	defer func() {
		if v := recover(); v != nil {
			exit = &proc.PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
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

func (r *runner[S]) system(m sysMsg) {
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
}
