package actor

import (
	"context"
	"fmt"
)

// Requests bound to their server and to the type of their reply: a
// request type embeds Req, the server takes Of its type, and the caller
// calls through a Ref, the type of the reply inferred from the request.
// The compiler then rejects a request sent to another server, a reply
// taken as another type, and a reply of another type.
//
//	type Get struct{ actor.Req[Calc, int] }
//	type Div struct {
//		actor.Req[Calc, int]
//		By int
//	}
//
//	func (Calc) HandleCall(n int, req actor.Of[Calc], from genserver.From[any]) (int, []actor.Effect) {
//		switch r := req.(type) {
//		case Div:
//			n /= r.By
//			return n, actor.Do(r.Reply(from, n))
//		}
//		return n, actor.Do(actor.Unknown(from, req))
//	}
//
//	calc := actor.NewRef[Calc](actor.Local("calc"))
//	n, err := calc.Call(ctx, caller, Div{By: 2}) // n is an int
//
// A switch on the requests is not checked to have them all: Unknown, in
// its default, replies to one left out rather than leaving its caller
// waiting. A reply made otherwise, as with the Reply of genserver.From,
// is not checked either.

// Req, embedded in a request type, makes it a request to the server Srv,
// replied a Rep. It is empty: a request with it is encoded as without.
type Req[Srv, Rep any] struct{}

func (Req[Srv, Rep]) server(Srv) {}
func (Req[Srv, Rep]) reply(Rep)  {}

// Reply returns the effect replying v to the call from, of this request.
func (Req[Srv, Rep]) Reply(from replyTo, v Rep) Effect {
	return Reply{To: from.replyTo(), Value: v}
}

// Of is a request to the server Srv, of any reply: what the server takes.
type Of[Srv any] interface{ server(Srv) }

// request is a request to Srv, replied a Rep.
type request[Srv, Rep any] interface {
	server(Srv)
	reply(Rep)
}

// replyTo is where a reply goes: a From, or one embedding it, as the From
// of genserver.
type replyTo interface{ replyTo() From }

func (f From) replyTo() From { return f }

// Ref is where the server Srv is, to call with its requests.
type Ref[Srv any] struct{ dest Dest }

// NewRef makes a Ref to the server Srv at dest, a PID or a name.
func NewRef[Srv any](dest Dest) Ref[Srv] { return Ref[Srv]{dest} }

// Dest returns where the server is.
func (r Ref[Srv]) Dest() Dest { return r.dest }

// Call calls the server with req, and returns its reply, of the type of
// req. See Call.
func (r Ref[Srv]) Call[Rep any](ctx context.Context, caller Caller, req request[Srv, Rep]) (Rep, error) {
	var zero Rep
	v, err := Call(ctx, caller, r.dest, req)
	switch e := v.(type) {
	case nil:
		return zero, err
	case *UnknownRequestError:
		return zero, e
	case Rep:
		return e, err
	}
	return zero, fmt.Errorf("actor: reply of type %T, want %T", v, zero)
}

// UnknownRequestError is the reply of Unknown, the error of Ref.Call.
type UnknownRequestError struct {
	Req any
}

func (e *UnknownRequestError) Error() string {
	return fmt.Sprintf("actor: request %T unknown to the server", e.Req)
}

// Unknown returns the effect replying to the call from that its server
// does not know req: for the default of a switch on its requests.
func Unknown(from replyTo, req any) Effect {
	return Reply{To: from.replyTo(), Value: &UnknownRequestError{Req: req}}
}
