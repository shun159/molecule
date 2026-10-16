package gen

import (
	"context"
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

	monitors map[any]proc.Ref // tag -> ref
	tags     map[proc.Ref]any // ref -> tag
	timers   map[any]timer    // key -> timer
	timerGen uint64
	asyncs   map[any]async // key -> async running, or waiting
	asyncGen uint64
	maxAsync int             // the most running at once, if > 0
	running  int             // asyncs running, cancelled ones until they return
	waiting  []queued        // asyncs waiting for one running to end, in order
	draining map[uint64]bool // gens of asyncs cancelled, still running
	requests map[proc.Ref]request

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
		maxAsync: o.MaxAsync,
		monitors: make(map[any]proc.Ref),
		tags:     make(map[proc.Ref]any),
		timers:   make(map[any]timer),
		asyncs:   make(map[any]async),
		draining: make(map[uint64]bool),
		requests: make(map[proc.Ref]request),
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
	if e, ok := msg.(proc.ExitMsg); ok && e.From == r.env.Parent() && !e.From.IsZero() {
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
		// Already boxed in msg: returning it, not m, spares boxing it
		// again.
		return msg, true
	case timeout:
		t, ok := r.timers[m.key]
		if !ok || t.gen != m.gen {
			return nil, false
		}
		delete(r.timers, m.key)
		return InfoMsg{Msg: t.msg}, true
	case asyncDone:
		a, ok := r.asyncs[m.key]
		if !ok || a.gen != m.gen {
			// Cancelled, or replaced: its outcome is not for the
			// behaviour, but its place is free now it has returned.
			if r.draining[m.gen] {
				delete(r.draining, m.gen)
				r.running--
				r.runWaiting()
			}
			return nil, false
		}
		delete(r.asyncs, m.key)
		r.running--
		r.runWaiting()
		return InfoMsg{Msg: actor.AsyncResult{Key: m.key, Value: m.value, Err: m.err}}, true
	case answer:
		req, ok := r.requests[m.ref]
		if !ok {
			return nil, false // timed out already
		}
		r.endRequest(m.ref, req)
		resp := actor.Response{Tag: req.tag, Value: m.m.Msg}
		if m.m.Down {
			resp = actor.Response{Tag: req.tag, Err: &actor.ExitError{To: req.to, Reason: m.m.Reason}}
		}
		r.callDone(req, resp.Err)
		return InfoMsg{Msg: resp}, true
	case requestTimeout:
		req, ok := r.requests[m.ref]
		if !ok {
			return nil, false // answered already
		}
		r.endRequest(m.ref, req)
		r.abandon(req.to, m.ref)
		r.callDone(req, context.DeadlineExceeded)
		return InfoMsg{Msg: actor.Response{Tag: req.tag, Err: context.DeadlineExceeded}}, true
	case responseMsg:
		return InfoMsg{Msg: m.Response}, true
	case downMsg:
		return InfoMsg{Msg: m.Down}, true
	case proc.DownMsg:
		tag, ok := r.tags[m.Ref]
		if !ok {
			return InfoMsg{Msg: m}, true
		}
		delete(r.tags, m.Ref)
		delete(r.monitors, tag)
		return InfoMsg{Msg: actor.Down{Tag: tag, PID: m.PID, Reason: m.Reason}}, true
	}
	return InfoMsg{Msg: msg}, true
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

func (r *runner[S]) Abort() {
	for key := range r.timers {
		r.cancelTimer(key)
	}
	for key := range r.asyncs {
		r.cancelAsync(key)
	}
	r.waiting = nil
	for ref, req := range r.requests {
		r.endRequest(ref, req)
	}
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
