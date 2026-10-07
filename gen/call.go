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

// CallMsg is the message a server receives for Call.
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
	n := caller.Node()
	pid, ok := to.WhereIs(n)
	if !ok {
		return nil, &ExitError{To: to, Reason: proc.NoProc}
	}

	var from From
	if p, ok := caller.(interface{ PID() proc.PID }); ok {
		from.PID = p.PID()
		if from.PID == pid {
			return nil, ErrCallingSelf
		}
	}
	var callerCtx context.Context = context.Background()
	if c, ok := caller.(interface{ Context() context.Context }); ok {
		callerCtx = c.Context()
	}

	// Watch first: if the server is already gone, there is nothing to send.
	down, stop := n.Watch(ctx, pid)
	defer stop()
	tag, replies, unalias := n.Alias()
	defer unalias()
	from.Tag = tag
	n.Send(pid, CallMsg{From: from, Req: req})
	return awaitReply(ctx, callerCtx, to, replies, down)
}

// awaitReply waits for the reply, the server's death, ctx, or the caller's
// death, whichever comes first.
func awaitReply(ctx, callerCtx context.Context, to Dest, replies <-chan any, down context.Context) (any, error) {
	select {
	case v := <-replies:
		return v, nil
	case <-down.Done():
		select {
		case v := <-replies:
			return v, nil
		default:
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, &ExitError{To: to, Reason: context.Cause(down)}
	case <-callerCtx.Done():
		return nil, context.Cause(callerCtx)
	}
}

// Cast sends req to the server at to without waiting, like gen_server:cast.
// It never fails: a request to a server that does not exist is dropped.
func Cast(caller Caller, to Dest, req any) {
	n := caller.Node()
	if pid, ok := to.WhereIs(n); ok {
		n.Send(pid, CastMsg{Req: req})
	}
}

// Reply sends v as the reply to the call from. Only the first reply to a
// call is delivered, and none once the caller has stopped waiting.
func Reply(caller Caller, from From, v any) {
	caller.Node().SendAlias(from.Tag, v)
}
