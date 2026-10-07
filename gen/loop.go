package gen

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/shun159/molecule/proc"
)

// Messages the runtime sends to its own process.
type (
	timeout struct {
		key any
		gen uint64
	}
	responseMsg struct{ Response }
	downMsg     struct{ Down }
)

// runtime runs a Behaviour in the current process and performs its
// effects. It is the only impure part of a behaviour.
type runtime[S any] struct {
	self  *proc.Self
	b     Behaviour[S]
	state S

	monitors map[any]proc.Ref // tag -> ref
	tags     map[proc.Ref]any // ref -> tag
	timers   map[any]timer    // key -> timer
	timerGen uint64

	stopping   bool
	stopReason error

	suspended bool
	deferred  []any
}

type timer struct {
	t   *time.Timer
	gen uint64
	msg any
}

func run[S any](self *proc.Self, b Behaviour[S], args any, o options) error {
	if o.name != nil {
		if err := o.name.Register(self.Node(), self.PID()); err != nil {
			if pid, ok := o.name.WhereIs(self.Node()); ok && pid != self.PID() {
				err = &AlreadyStartedError{PID: pid}
			}
			self.InitAck(err)
			return nil
		}
	}

	r := &runtime[S]{
		self:     self,
		b:        b,
		monitors: make(map[any]proc.Ref),
		tags:     make(map[proc.Ref]any),
		timers:   make(map[any]timer),
	}
	state, effs, err := r.init(args)
	if err != nil {
		self.InitAck(err)
		if errors.Is(err, ErrIgnore) {
			return nil
		}
		return err
	}
	r.state = state
	r.apply(effs)
	if r.stopping {
		r.stopTimers()
		self.InitAck(r.stopReason)
		return r.stopReason
	}
	self.InitAck(nil)
	return r.loop()
}

func (r *runtime[S]) init(args any) (state S, effs []Effect, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &proc.PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	return r.b.Init(args)
}

func (r *runtime[S]) loop() error {
	for {
		msg, err := r.next()
		if err != nil {
			r.stopTimers()
			return err // killed: no Terminate, as in Erlang
		}
		if m, ok := msg.(sysMsg); ok {
			r.system(m)
			continue
		}
		if e, ok := msg.(proc.ExitMsg); ok && e.From == r.self.Parent() && !e.From.IsZero() {
			return r.terminate(r.state, e.Reason)
		}
		if r.suspended {
			r.deferred = append(r.deferred, msg)
			continue
		}
		in, ok := r.translate(msg)
		if !ok {
			continue
		}
		state, effs, err := r.handle(in)
		if err != nil {
			return r.terminate(r.state, err)
		}
		r.state = state
		r.apply(effs)
		if r.stopping {
			return r.terminate(r.state, r.stopReason)
		}
	}
}

// next returns the next message: a deferred one once resumed, or one from
// the mailbox.
func (r *runtime[S]) next() (any, error) {
	if !r.suspended && len(r.deferred) > 0 {
		if ctx := r.self.Context(); ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		msg := r.deferred[0]
		r.deferred[0] = nil
		r.deferred = r.deferred[1:]
		return msg, nil
	}
	return r.self.Receive(context.Background())
}

func (r *runtime[S]) handle(in Msg) (state S, effs []Effect, err error) {
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
func (r *runtime[S]) translate(msg any) (Msg, bool) {
	switch m := msg.(type) {
	case CallMsg:
		return m, true
	case CastMsg:
		return m, true
	case timeout:
		t, ok := r.timers[m.key]
		if !ok || t.gen != m.gen {
			return nil, false
		}
		delete(r.timers, m.key)
		return InfoMsg{Msg: t.msg}, true
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
		return InfoMsg{Msg: Down{Tag: tag, PID: m.PID, Reason: m.Reason}}, true
	}
	return InfoMsg{Msg: msg}, true
}

// terminate runs Terminate and returns the reason to exit with.
func (r *runtime[S]) terminate(state S, reason error) (exit error) {
	defer r.stopTimers()
	defer func() {
		if v := recover(); v != nil {
			exit = &proc.PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	effs := r.b.Terminate(state, reason)
	r.stopping = true // Stop has no further effect
	r.apply(effs)
	return reason
}

func (r *runtime[S]) system(m sysMsg) {
	var reply any
	switch m.Req {
	case sysGetState:
		reply = r.state
	case sysSuspend:
		r.suspended = true
	case sysResume:
		r.suspended = false
	}
	SendReply(r.self, m.From, reply)
}

func (r *runtime[S]) apply(effs []Effect) {
	for _, e := range effs {
		switch e := e.(type) {
		case Reply:
			SendReply(r.self, e.To, e.Value)
		case Send:
			if pid, ok := e.To.WhereIs(r.self.Node()); ok {
				r.self.Send(pid, e.Msg)
			}
		case Cast:
			SendCast(r.self, e.To, e.Req)
		case Stop:
			if !r.stopping {
				r.stopping = true
				r.stopReason = e.Reason
				if r.stopReason == nil {
					r.stopReason = proc.Normal
				}
			}
		case Monitor:
			r.monitor(e)
		case Demonitor:
			r.demonitor(e.Tag)
		case StartTimer:
			r.startTimer(e)
		case CancelTimer:
			r.cancelTimer(e.Key)
		case SendRequest:
			r.sendRequest(e)
		case Link:
			r.self.Link(e.PID)
		case Unlink:
			r.self.Unlink(e.PID)
		case TrapExit:
			r.self.TrapExit(e.On)
		default:
			panic(fmt.Sprintf("gen: unknown effect %T", e))
		}
	}
}

func (r *runtime[S]) monitor(e Monitor) {
	r.demonitor(e.Tag)
	pid, ok := e.Target.WhereIs(r.self.Node())
	if !ok {
		r.self.Send(r.self.PID(), downMsg{Down{Tag: e.Tag, Reason: proc.NoProc}})
		return
	}
	ref := r.self.Monitor(pid)
	r.monitors[e.Tag] = ref
	r.tags[ref] = e.Tag
}

func (r *runtime[S]) demonitor(tag any) {
	if ref, ok := r.monitors[tag]; ok {
		r.self.Demonitor(ref)
		delete(r.monitors, tag)
		delete(r.tags, ref)
	}
}

func (r *runtime[S]) startTimer(e StartTimer) {
	r.cancelTimer(e.Key)
	r.timerGen++
	g, n, self := r.timerGen, r.self.Node(), r.self.PID()
	t := time.AfterFunc(e.After, func() { n.Send(self, timeout{key: e.Key, gen: g}) })
	r.timers[e.Key] = timer{t: t, gen: g, msg: e.Msg}
}

func (r *runtime[S]) cancelTimer(key any) {
	if t, ok := r.timers[key]; ok {
		t.t.Stop()
		delete(r.timers, key)
	}
}

func (r *runtime[S]) stopTimers() {
	for key := range r.timers {
		r.cancelTimer(key)
	}
}

func (r *runtime[S]) sendRequest(e SendRequest) {
	n, self := r.self.Node(), r.self.PID()
	respond := func(v any, err error) {
		n.Send(self, responseMsg{Response{Tag: e.Tag, Value: v, Err: err}})
	}
	pid, ok := e.To.WhereIs(n)
	if !ok {
		respond(nil, &ExitError{To: e.To, Reason: proc.NoProc})
		return
	}

	ctx, cancel := context.Background(), context.CancelFunc(func() {})
	if e.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, e.Timeout)
	}
	req := request(ctx, n, self, pid, e.To, func(f From) any { return CallMsg{From: f, Req: e.Req} })
	callerCtx := r.self.Context()
	go func() {
		defer cancel()
		defer req.release()
		respond(awaitReply(ctx, callerCtx, e.To, req.replies, req.down))
	}()
}
