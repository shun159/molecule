package gen

import (
	"context"
	"errors"
	"fmt"

	"github.com/shun159/molecule/proc"
)

// From identifies a pending call, to reply to with Reply. It is plain data:
// the caller's PID (zero when the caller is not a process) and the alias
// the reply goes to.
type From struct {
	PID proc.PID
	Tag proc.Ref
}

// CallMsg is the message a server receives for Call. Behaviours get it
// as their Msg.
type CallMsg struct {
	From From
	Req  any
}

// CastMsg is the message a server receives for Cast.
type CastMsg struct {
	Req any
}

// Caller is who makes a call or a cast: a process (*proc.Self) or, for
// code outside processes, a *proc.Node.
type Caller interface {
	Node() *proc.Node
}

// ErrCallingSelf is returned by a process that calls itself, which would
// otherwise wait forever for its own reply.
var ErrCallingSelf = errors.New("gen: calling self")

// ExitError is returned by Call when the server died, or never existed,
// before replying. It unwraps to the exit reason, so
// errors.Is(err, proc.NoProc) tells that there was no server.
type ExitError struct {
	To     Dest
	Reason error
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("gen: call to %v: %v", e.To, e.Reason)
}

func (e *ExitError) Unwrap() error { return e.Reason }

// Call sends req to the server at to and waits for its reply, like
// gen:call. It returns an *ExitError if the server dies or does not exist,
// and ctx.Err() if ctx is done first. A caller that is a process stops
// waiting as soon as it dies itself. A reply sent before the server died
// is still returned.
func Call(ctx context.Context, caller Caller, to Dest, req any) (any, error) {
	return call(ctx, caller, to, func(f From) any { return CallMsg{From: f, Req: req} })
}

// call makes a synchronous request; wrap builds the message to send.
func call(ctx context.Context, caller Caller, to Dest, wrap func(From) any) (any, error) {
	n := caller.Node()
	pid, ok := to.WhereIs(n)
	if !ok {
		return nil, &ExitError{To: to, Reason: proc.NoProc}
	}
	var self proc.PID
	if p, ok := caller.(interface{ PID() proc.PID }); ok {
		self = p.PID()
		if self == pid {
			return nil, ErrCallingSelf
		}
	}
	callerCtx := context.Background()
	if c, ok := caller.(interface{ Context() context.Context }); ok {
		callerCtx = c.Context()
	}

	a := n.MonitorAlias(pid)
	defer a.Release()
	n.Send(pid, wrap(From{PID: self, Tag: a.Ref}))
	return awaitReply(ctx, callerCtx, to, a.C)
}

// awaitReply waits for the reply or the death of the server, both arriving
// on c, for ctx, or for the death of the caller, whichever comes first.
func awaitReply(ctx, callerCtx context.Context, to Dest, c <-chan proc.AliasMsg) (any, error) {
	select {
	case m := <-c:
		if m.Down {
			return nil, &ExitError{To: to, Reason: m.Reason}
		}
		return m.Msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-callerCtx.Done():
		return nil, context.Cause(callerCtx)
	}
}

// SendCast sends req to the server at to without waiting, like
// gen_server:cast. It never fails: a request to a server that does not
// exist is dropped. Behaviours return a Cast effect instead.
func SendCast(caller Caller, to Dest, req any) {
	n := caller.Node()
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
