package molecule

import (
	"context"
	"errors"
	"fmt"

	"github.com/shun159/molecule/proc"
)

// Caller is who makes a call or a cast: a process (*proc.Self) or, for
// code outside processes, a *proc.Node.
type Caller interface {
	Node() *proc.Node
}

// ErrCallingSelf is returned by a process that calls itself, which would
// otherwise wait forever for its own reply.
var ErrCallingSelf = errors.New("molecule: calling self")

// ExitError is returned by Call when the server died, or never existed,
// before replying. It unwraps to the exit reason, so
// errors.Is(err, proc.NoProc) tells that there was no server.
type ExitError struct {
	To     Dest
	Reason error
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("molecule: call to %v: %v", e.To, e.Reason)
}

func (e *ExitError) Unwrap() error { return e.Reason }

// Call sends req to the server at to and waits for its reply, like
// gen:call. It returns an *ExitError if the server dies or does not exist,
// and ctx.Err() if ctx is done first. A caller that is a process stops
// waiting as soon as it dies itself. A reply sent before the server died
// is still returned.
//
// A caller that stops waiting before the reply -- ctx done, or the caller
// dead -- tells the server so with a CallAbandoned.
func Call(ctx context.Context, caller Caller, to Dest, req any) (any, error) {
	return call(ctx, caller, to, func(f From) any { return CallMsg{From: f, Req: req} }, true)
}

// CallWith is Call with the message wrap makes of the From, for the
// runtimes of behaviours with messages of their own, as the system
// messages of gen, like the label of gen:call in Erlang.
func CallWith(ctx context.Context, caller Caller, to Dest, wrap func(From) any) (any, error) {
	return call(ctx, caller, to, wrap, false)
}

// call makes a synchronous request; wrap builds the message to send. With
// tell, giving up tells the server.
func call(ctx context.Context, caller Caller, to Dest, wrap func(From) any, tell bool) (any, error) {
	a, abandon, err := sendCall(caller, to, wrap)
	if err != nil {
		return nil, err
	}
	defer a.Release()
	d, _ := caller.(dying)
	v, gaveUp, err := awaitReply(ctx, d, to, a.C)
	if gaveUp && tell {
		abandon()
	}
	return v, err
}

// sendCall sends a request, and returns the alias its reply arrives on,
// or the death of the server: the server is monitored, by the alias,
// before the request is sent. abandon tells the server the caller gave
// up.
func sendCall(caller Caller, to Dest, wrap func(From) any) (a proc.Alias, abandon func(), err error) {
	n := caller.Node()
	var self proc.PID
	if p, ok := caller.(interface{ PID() proc.PID }); ok {
		self = p.PID()
	}
	if r, ok := to.(Remote); ok && r.Node != n.Name() {
		// The name is resolved there, and monitored there first.
		a := n.MonitorAliasName(r.Node, r.Name)
		from := From{PID: self, Tag: a.Ref}
		n.SendName(r.Node, r.Name, wrap(from))
		return a, func() { n.SendName(r.Node, r.Name, CallAbandoned{From: from}) }, nil
	}
	pid, ok := to.WhereIs(n)
	if !ok {
		return proc.Alias{}, nil, &ExitError{To: to, Reason: proc.NoProc}
	}
	if !self.IsZero() && self == pid {
		return proc.Alias{}, nil, ErrCallingSelf
	}
	a = n.MonitorAlias(pid)
	from := From{PID: self, Tag: a.Ref}
	n.Send(pid, wrap(from))
	return a, func() { n.Send(pid, CallAbandoned{From: from}) }, nil
}

// dying is a caller that may die while it waits: a process.
type dying interface {
	Done() <-chan struct{}
	ExitReason() error
}

// awaitReply waits for the reply or the death of the server, both arriving
// on c, for ctx, or for the death of the caller, whichever comes first. It
// reports whether the caller gave up: ctx or the caller done first.
func awaitReply(ctx context.Context, caller dying, to Dest, c <-chan proc.AliasMsg) (v any, gaveUp bool, err error) {
	var callerDone <-chan struct{}
	if caller != nil {
		callerDone = caller.Done()
	}
	select {
	case m := <-c:
		if m.Down {
			return nil, false, &ExitError{To: to, Reason: m.Reason}
		}
		return m.Msg, false, nil
	case <-ctx.Done():
		return nil, true, ctx.Err()
	case <-callerDone:
		return nil, true, caller.ExitReason()
	}
}

// SendCast sends req to the server at to without waiting, like
// gen_server:cast. It never fails: a request to a server that does not
// exist is dropped. Behaviours return a Cast effect instead.
func SendCast(caller Caller, to Dest, req any) {
	n := caller.Node()
	if r, ok := to.(Remote); ok {
		n.SendName(r.Node, r.Name, CastMsg{Req: req})
		return
	}
	if pid, ok := to.WhereIs(n); ok {
		n.Send(pid, CastMsg{Req: req})
	}
}

// SendReply sends v as the reply to the call from. Only the first reply to
// a call is delivered, and none once the caller has stopped waiting.
// Behaviours return a Reply effect instead.
func SendReply(caller Caller, from From, v any) {
	caller.Node().SendAlias(from.Tag, v)
}
