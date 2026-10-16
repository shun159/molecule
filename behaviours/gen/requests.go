package gen

import (
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

func (r *runner[S]) sendRequest(e actor.SendRequest) {
	if to, ok := e.To.(actor.Remote); ok && to.Node != r.env.Self().Node() {
		// The name is resolved there, and monitored there first.
		ref, release := r.env.RequestName(to.Node, to.Name, func(ref proc.Ref, m proc.AliasMsg) any {
			return answer{ref: ref, m: m}
		})
		r.request(e, ref, release)
		r.env.SendName(to.Node, to.Name, actor.CallMsg{From: actor.From{PID: r.env.Self(), Tag: ref}, Req: e.Req})
		return
	}
	pid, ok := r.env.Resolve(e.To)
	if !ok {
		err := &actor.ExitError{To: e.To, Reason: proc.NoProc}
		r.env.Emit(actor.CallDone{To: e.To, Req: e.Req, Err: err})
		r.env.Send(r.env.Self(), responseMsg{actor.Response{Tag: e.Tag, Err: err}})
		return
	}
	ref, release := r.env.Request(pid, func(ref proc.Ref, m proc.AliasMsg) any {
		return answer{ref: ref, m: m}
	})
	r.request(e, ref, release)
	r.env.Send(pid, actor.CallMsg{From: actor.From{PID: r.env.Self(), Tag: ref}, Req: e.Req})
}

// request records the request e, made under ref.
func (r *runner[S]) request(e actor.SendRequest, ref proc.Ref, release func()) {
	req := request{tag: e.Tag, to: e.To, req: e.Req, start: r.env.Now(), release: release}
	if e.Timeout > 0 {
		req.cancel = r.env.SendAfter(e.Timeout, requestTimeout{ref: ref})
	}
	r.requests[ref] = req
}

// abandon tells the server at to that the request made under ref is given
// up on, as actor.Call does.
func (r *runner[S]) abandon(to actor.Dest, ref proc.Ref) {
	msg := actor.CallAbandoned{From: actor.From{PID: r.env.Self(), Tag: ref}}
	if rem, ok := to.(actor.Remote); ok && rem.Node != r.env.Self().Node() {
		r.env.SendName(rem.Node, rem.Name, msg)
	} else if pid, ok := r.env.Resolve(to); ok {
		r.env.Send(pid, msg)
	}
}

// callDone emits the CallDone of req, ended with err.
func (r *runner[S]) callDone(req request, err error) {
	r.env.Emit(actor.CallDone{To: req.to, Req: req.req, Took: r.env.Now().Sub(req.start), Err: err})
}

func (r *runner[S]) endRequest(ref proc.Ref, req request) {
	delete(r.requests, ref)
	req.release()
	if req.cancel != nil {
		req.cancel()
	}
}
