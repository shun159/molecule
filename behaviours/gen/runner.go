package gen

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

// Messages the runtime sends to its own process.
type (
	timeout struct {
		key any
		gen uint64
	}
	downMsg     struct{ molecule.Down }
	responseMsg struct{ molecule.Response }
	// answer is the reply to a SendRequest, or the exit of its server.
	answer struct {
		ref proc.Ref
		m   proc.AliasMsg
	}
	// requestTimeout ends a SendRequest still waiting.
	requestTimeout struct{ ref proc.Ref }
)

// runtime runs a Behaviour and performs its effects through an Env. It
// holds the meaning of the effects; the Env, what they act on.
type runtime[S any] struct {
	env   Env
	b     Behaviour[S]
	state S

	monitors map[any]proc.Ref // tag -> ref
	tags     map[proc.Ref]any // ref -> tag
	timers   map[any]timer    // key -> timer
	timerGen uint64
	requests map[proc.Ref]request

	stopping   bool
	stopReason error

	suspended bool
	deferred  []any

	last any // the message being handled, for the report on termination

	continues []any // Msgs of Continue effects, handled before the mailbox
}

type timer struct {
	cancel func()
	gen    uint64
	msg    any
}

// request is a SendRequest waiting for its answer.
type request struct {
	tag     any
	to      molecule.Dest
	release func()
	cancel  func() // of its timeout, if any
}

func newRuntime[S any](b Behaviour[S], env Env) *runtime[S] {
	return &runtime[S]{
		env:      env,
		b:        b,
		monitors: make(map[any]proc.Ref),
		tags:     make(map[proc.Ref]any),
		timers:   make(map[any]timer),
		requests: make(map[proc.Ref]request),
	}
}

func (r *runtime[S]) Init(args any) error {
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
	return nil
}

func (r *runtime[S]) init(args any) (state S, effs []molecule.Effect, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &proc.PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	return r.b.Init(r.env.Self(), args)
}

func (r *runtime[S]) State() any { return r.state }

func (r *runtime[S]) Deliver(msg any) (bool, error) {
	if done, reason := r.receive(msg); done {
		return true, reason
	}
	return r.Flush()
}

func (r *runtime[S]) Flush() (bool, error) {
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
func (r *runtime[S]) receive(msg any) (bool, error) {
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
func (r *runtime[S]) step(in Msg) (bool, error) {
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

func (r *runtime[S]) handle(in Msg) (state S, effs []molecule.Effect, err error) {
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
	case molecule.CallMsg, molecule.CastMsg:
		// Already boxed in msg: asserting reuses it, where converting m
		// would box it again.
		return msg.(Msg), true
	case timeout:
		t, ok := r.timers[m.key]
		if !ok || t.gen != m.gen {
			return nil, false
		}
		delete(r.timers, m.key)
		return InfoMsg{Msg: t.msg}, true
	case answer:
		req, ok := r.requests[m.ref]
		if !ok {
			return nil, false // timed out already
		}
		r.endRequest(m.ref, req)
		resp := molecule.Response{Tag: req.tag, Value: m.m.Msg}
		if m.m.Down {
			resp = molecule.Response{Tag: req.tag, Err: &molecule.ExitError{To: req.to, Reason: m.m.Reason}}
		}
		return InfoMsg{Msg: resp}, true
	case requestTimeout:
		req, ok := r.requests[m.ref]
		if !ok {
			return nil, false // answered already
		}
		r.endRequest(m.ref, req)
		return InfoMsg{Msg: molecule.Response{Tag: req.tag, Err: context.DeadlineExceeded}}, true
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
		return InfoMsg{Msg: molecule.Down{Tag: tag, PID: m.PID, Reason: m.Reason}}, true
	}
	return InfoMsg{Msg: msg}, true
}

// terminate runs Terminate and returns the reason to exit with.
func (r *runtime[S]) terminate(state S, reason error) (exit error) {
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

func (r *runtime[S]) Abort() {
	for key := range r.timers {
		r.cancelTimer(key)
	}
	for ref, req := range r.requests {
		r.endRequest(ref, req)
	}
}

// report logs that the behaviour terminates abnormally, with the message
// it was handling and its state, like the report of a terminating
// gen_server. The crash report of the process follows.
func (r *runtime[S]) report(state S, reason error) {
	st := molecule.Status{State: state, Message: r.last, Reason: reason}
	if f, ok := any(r.b).(molecule.StatusFormatter); ok {
		st = f.FormatStatus(st)
	}
	attrs := []any{
		slog.String("pid", r.env.Self().String()),
		slog.String("behaviour", fmt.Sprintf("%T", r.b)),
		slog.String("last_message", brief(st.Message)),
		slog.String("state", brief(st.State)),
	}
	if st.Reason != nil {
		attrs = append(attrs, slog.String("reason", st.Reason.Error()))
	}
	r.env.Logger().Error("behaviour terminating", attrs...)
}

// briefLimit bounds the size of values in reports.
const briefLimit = 1024

// brief formats v for a report, cut short if long.
func brief(v any) string {
	s := fmt.Sprintf("%+v", v)
	if len(s) > briefLimit {
		return s[:briefLimit] + "..."
	}
	return s
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
	r.env.SendAlias(m.From.Tag, reply)
}

func (r *runtime[S]) apply(effs []molecule.Effect) {
	for _, e := range effs {
		switch e := e.(type) {
		case molecule.Continue:
			r.continues = append(r.continues, e.Msg)
		case molecule.Reply:
			r.env.SendAlias(e.To.Tag, e.Value)
		case molecule.Send:
			if to, ok := e.To.(molecule.Remote); ok {
				r.env.SendName(to.Node, to.Name, e.Msg)
			} else if pid, ok := r.env.Resolve(e.To); ok {
				r.env.Send(pid, e.Msg)
			}
		case molecule.Cast:
			if to, ok := e.To.(molecule.Remote); ok {
				r.env.SendName(to.Node, to.Name, molecule.CastMsg{Req: e.Req})
			} else if pid, ok := r.env.Resolve(e.To); ok {
				r.env.Send(pid, molecule.CastMsg{Req: e.Req})
			}
		case molecule.Stop:
			if !r.stopping {
				r.stopping = true
				r.stopReason = e.Reason
				if r.stopReason == nil {
					r.stopReason = proc.Normal
				}
			}
		case molecule.Monitor:
			r.monitor(e)
		case molecule.Demonitor:
			r.demonitor(e.Tag)
		case molecule.StartTimer:
			r.startTimer(e)
		case molecule.CancelTimer:
			r.cancelTimer(e.Key)
		case molecule.SendRequest:
			r.sendRequest(e)
		case molecule.Link:
			r.env.Link(e.PID)
		case molecule.Unlink:
			r.env.Unlink(e.PID)
		case molecule.TrapExit:
			r.env.TrapExit(e.On)
		case molecule.MonitorNodes:
			r.env.MonitorNodes(e.On)
		case Performer:
			e.Perform(r.env)
		default:
			panic(fmt.Sprintf("gen: unknown effect %T", e))
		}
	}
}

func (r *runtime[S]) monitor(e molecule.Monitor) {
	r.demonitor(e.Tag)
	if to, ok := e.Target.(molecule.Remote); ok && to.Node != r.env.Self().Node() {
		ref := r.env.MonitorName(to.Node, to.Name)
		r.monitors[e.Tag] = ref
		r.tags[ref] = e.Tag
		return
	}
	pid, ok := r.env.Resolve(e.Target)
	if !ok {
		r.env.Send(r.env.Self(), downMsg{molecule.Down{Tag: e.Tag, Reason: proc.NoProc}})
		return
	}
	ref := r.env.Monitor(pid)
	r.monitors[e.Tag] = ref
	r.tags[ref] = e.Tag
}

func (r *runtime[S]) demonitor(tag any) {
	if ref, ok := r.monitors[tag]; ok {
		r.env.Demonitor(ref)
		delete(r.monitors, tag)
		delete(r.tags, ref)
	}
}

func (r *runtime[S]) startTimer(e molecule.StartTimer) {
	r.cancelTimer(e.Key)
	r.timerGen++
	after := e.After
	if !e.At.IsZero() {
		after = max(0, e.At.Sub(r.env.Now()))
	}
	cancel := r.env.SendAfter(after, timeout{key: e.Key, gen: r.timerGen})
	r.timers[e.Key] = timer{cancel: cancel, gen: r.timerGen, msg: e.Msg}
}

func (r *runtime[S]) cancelTimer(key any) {
	if t, ok := r.timers[key]; ok {
		t.cancel()
		delete(r.timers, key)
	}
}

func (r *runtime[S]) sendRequest(e molecule.SendRequest) {
	if to, ok := e.To.(molecule.Remote); ok && to.Node != r.env.Self().Node() {
		// The name is resolved there, and monitored there first.
		ref, release := r.env.RequestName(to.Node, to.Name, func(ref proc.Ref, m proc.AliasMsg) any {
			return answer{ref: ref, m: m}
		})
		r.request(e, ref, release)
		r.env.SendName(to.Node, to.Name, molecule.CallMsg{From: molecule.From{PID: r.env.Self(), Tag: ref}, Req: e.Req})
		return
	}
	pid, ok := r.env.Resolve(e.To)
	if !ok {
		r.env.Send(r.env.Self(), responseMsg{molecule.Response{Tag: e.Tag, Err: &molecule.ExitError{To: e.To, Reason: proc.NoProc}}})
		return
	}
	ref, release := r.env.Request(pid, func(ref proc.Ref, m proc.AliasMsg) any {
		return answer{ref: ref, m: m}
	})
	r.request(e, ref, release)
	r.env.Send(pid, molecule.CallMsg{From: molecule.From{PID: r.env.Self(), Tag: ref}, Req: e.Req})
}

// request records the request e, made under ref.
func (r *runtime[S]) request(e molecule.SendRequest, ref proc.Ref, release func()) {
	req := request{tag: e.Tag, to: e.To, release: release}
	if e.Timeout > 0 {
		req.cancel = r.env.SendAfter(e.Timeout, requestTimeout{ref: ref})
	}
	r.requests[ref] = req
}

func (r *runtime[S]) endRequest(ref proc.Ref, req request) {
	delete(r.requests, ref)
	req.release()
	if req.cancel != nil {
		req.cancel()
	}
}
