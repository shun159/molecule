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

	r := request(ctx, n, self, pid, to, wrap)
	defer r.release()
	return awaitReply(ctx, callerCtx, to, r.replies, r.down)
}

// pending is a request sent and waiting for its reply.
type pending struct {
	down    context.Context
	replies <-chan any
	release func()
}

// request sends wrap(From) to pid, the resolved to, and returns what to
// wait on for the reply. release must be called once done waiting.
func request(ctx context.Context, n *proc.Node, self, pid proc.PID, to Dest, wrap func(From) any) pending {
	// Watch first: if the server is already gone, the reply never comes.
	down, stop := n.Watch(ctx, pid)
	tag, replies, unalias := n.Alias()
	n.Send(pid, wrap(From{PID: self, Tag: tag}))
	return pending{down: down, replies: replies, release: func() { unalias(); stop() }}
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
