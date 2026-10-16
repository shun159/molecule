package gen

import (
	"context"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// The messages of a SendRequest the runner sends itself.
type (
	// answer is the reply to a SendRequest, or the exit of its server.
	answer struct {
		ref proc.Ref
		m   proc.AliasMsg
	}
	// requestTimeout ends a SendRequest still waiting.
	requestTimeout struct{ ref proc.Ref }
	// responseMsg is the Response to a SendRequest that could not be sent.
	responseMsg struct{ actor.Response }
)

// request is a SendRequest waiting for its answer.
type request struct {
	tag     any
	to      actor.Dest
	req     any
	start   time.Time // for its CallDone
	release func()
	cancel  func() // of its timeout, if any
}

// requests are the SendRequests of a behaviour waiting for their answer,
// by the ref they are made under.
type requests struct {
	env   Env
	byRef map[proc.Ref]request
}

func newRequests(env Env) requests { return requests{env: env, byRef: make(map[proc.Ref]request)} }

// send sends the request of e, as a call: to a process of another node by
// its name, resolved there, or to one of this node.
func (q *requests) send(e actor.SendRequest) {
	if to, ok := e.To.(actor.Remote); ok && to.Node != q.env.Self().Node() {
		ref, release := q.env.RequestName(to.Node, to.Name, answerOf)
		q.add(e, ref, release)
		q.env.SendName(to.Node, to.Name, q.call(ref, e.Req))
		return
	}
	pid, ok := q.env.Resolve(e.To)
	if !ok {
		q.notSent(e)
		return
	}
	ref, release := q.env.Request(pid, answerOf)
	q.add(e, ref, release)
	q.env.Send(pid, q.call(ref, e.Req))
}

func answerOf(ref proc.Ref, m proc.AliasMsg) any { return answer{ref: ref, m: m} }

// call is the call of req, made under ref.
func (q *requests) call(ref proc.Ref, req any) actor.CallMsg {
	return actor.CallMsg{From: actor.From{PID: q.env.Self(), Tag: ref}, Req: req}
}

// notSent answers e at once, its server not found.
func (q *requests) notSent(e actor.SendRequest) {
	err := &actor.ExitError{To: e.To, Reason: proc.NoProc}
	q.env.Emit(actor.CallDone{To: e.To, Req: e.Req, Err: err})
	q.env.Send(q.env.Self(), responseMsg{actor.Response{Tag: e.Tag, Err: err}})
}

// add records the request e, made under ref, with its timeout, if any.
func (q *requests) add(e actor.SendRequest, ref proc.Ref, release func()) {
	req := request{tag: e.Tag, to: e.To, req: e.Req, start: q.env.Now(), release: release}
	if e.Timeout > 0 {
		req.cancel = q.env.SendAfter(e.Timeout, requestTimeout{ref: ref})
	}
	q.byRef[ref] = req
}

// answered returns the Response of the request of m: its reply, or the
// exit of its server. One timed out already is answered no more.
func (q *requests) answered(m answer) (Msg, bool) {
	req, ok := q.byRef[m.ref]
	if !ok {
		return nil, false
	}
	q.end(m.ref, req)
	resp := actor.Response{Tag: req.tag, Value: m.m.Msg}
	if m.m.Down {
		resp = actor.Response{Tag: req.tag, Err: &actor.ExitError{To: req.to, Reason: m.m.Reason}}
	}
	q.done(req, resp.Err)
	return InfoMsg{Msg: resp}, true
}

// timedOut gives up on the request of m, telling its server, and returns
// its Response, of the deadline. One answered already is not.
func (q *requests) timedOut(m requestTimeout) (Msg, bool) {
	req, ok := q.byRef[m.ref]
	if !ok {
		return nil, false
	}
	q.end(m.ref, req)
	q.abandon(req.to, m.ref)
	q.done(req, context.DeadlineExceeded)
	return InfoMsg{Msg: actor.Response{Tag: req.tag, Err: context.DeadlineExceeded}}, true
}

// abandon tells the server at to that the request made under ref is given
// up on, as actor.Call does.
func (q *requests) abandon(to actor.Dest, ref proc.Ref) {
	msg := actor.CallAbandoned{From: actor.From{PID: q.env.Self(), Tag: ref}}
	if rem, ok := to.(actor.Remote); ok && rem.Node != q.env.Self().Node() {
		q.env.SendName(rem.Node, rem.Name, msg)
	} else if pid, ok := q.env.Resolve(to); ok {
		q.env.Send(pid, msg)
	}
}

// done emits the CallDone of req, ended with err.
func (q *requests) done(req request, err error) {
	q.env.Emit(actor.CallDone{To: req.to, Req: req.req, Took: q.env.Now().Sub(req.start), Err: err})
}

// end forgets the request under ref, and what it holds.
func (q *requests) end(ref proc.Ref, req request) {
	delete(q.byRef, ref)
	req.release()
	if req.cancel != nil {
		req.cancel()
	}
}

// abort forgets them all, unanswered.
func (q *requests) abort() {
	for ref, req := range q.byRef {
		q.end(ref, req)
	}
}
