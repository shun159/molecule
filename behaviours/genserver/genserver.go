package genserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Behaviour is a server with state S that answers calls of type Req with
// replies of type Rep, and takes casts of type Cast. Configuration belongs
// in the fields of the implementing type, which Init reads.
//
// A Behaviour may also implement InfoHandler, ContinueHandler and
// Terminator.
type Behaviour[S, Req, Rep, Cast any] interface {
	// Init returns the initial state. self is the PID of the server.
	Init(self proc.PID) (S, []actor.Effect, error)
	// HandleCall handles a call. The reply is an effect made with
	// from.Reply, now or, keeping from in the state, later.
	HandleCall(state S, req Req, from From[Rep]) (S, []actor.Effect)
	HandleCast(state S, msg Cast) (S, []actor.Effect)
}

// Default gives a Behaviour the callbacks it has no use for, embedded in
// it: Init starts with the zero state, and a server without HandleCall or
// HandleCast takes no calls or no casts, one stopping it with a
// BadMessageError, as a request of the wrong type does.
//
//	// Log keeps what it is sent.
//	type Log struct{ genserver.Default[[]string] }
//
//	func (Log) HandleInfo(log []string, msg any) ([]string, []actor.Effect) {
//		return append(slices.Clip(log), fmt.Sprint(msg)), nil
//	}
type Default[S any] struct{}

// None is the request, reply or cast type of a server taking none, by
// Default: no value has it but nil, which no request carries.
type None interface{ none() }

func (Default[S]) Init(proc.PID) (S, []actor.Effect, error) {
	var zero S
	return zero, nil, nil
}

func (Default[S]) HandleCall(s S, _ None, _ From[None]) (S, []actor.Effect) { return s, nil }

func (Default[S]) HandleCast(s S, _ None) (S, []actor.Effect) { return s, nil }

// InfoHandler handles the messages that are neither calls nor casts, see
// gen.InfoMsg. Without it, they are dropped.
type InfoHandler[S any] interface {
	HandleInfo(state S, msg any) (S, []actor.Effect)
}

// ContinueHandler handles the Msg of a actor.Continue effect, right after
// the callback that returned it, like handle_continue in OTP. A server
// returning actor.Continue must implement it, or stops with
// ErrNoHandleContinue.
type ContinueHandler[S any] interface {
	HandleContinue(state S, msg any) (S, []actor.Effect)
}

// ErrNoHandleContinue is the exit reason of a server returning actor.Continue
// without implementing ContinueHandler.
var ErrNoHandleContinue = errors.New("genserver: actor.Continue without HandleContinue")

// Terminator is called when the server stops, see gen.Behaviour.
type Terminator[S any] interface {
	Terminate(state S, reason error) []actor.Effect
}

// From identifies a call awaiting a reply of type Rep.
type From[Rep any] struct {
	actor.From
}

// Reply returns the effect that replies v to the call.
func (f From[Rep]) Reply(v Rep) actor.Effect {
	return actor.Reply{To: f.From, Value: v}
}

// BadMessageError is the exit reason of a server that got a call or cast
// of the wrong type, which only raw actor.Call or actor.SendCast can send.
type BadMessageError struct {
	Msg any
}

func (e *BadMessageError) Error() string {
	return fmt.Sprintf("genserver: unexpected %T", e.Msg)
}

// Ref is a typed handle on a server.
type Ref[Req, Rep, Cast any] struct {
	dest actor.Dest
}

// NewRef makes a Ref to the server at dest, such as a actor.Local name.
func NewRef[Req, Rep, Cast any](dest actor.Dest) Ref[Req, Rep, Cast] {
	return Ref[Req, Rep, Cast]{dest: dest}
}

// RefFor makes a Ref to the server running b at dest, taking its types
// from b: genserver.RefFor(Counter{}, actor.Local("counter")).
func RefFor[S, Req, Rep, Cast any](_ Behaviour[S, Req, Rep, Cast], dest actor.Dest) Ref[Req, Rep, Cast] {
	return NewRef[Req, Rep, Cast](dest)
}

// Dest returns where the server is.
func (r Ref[Req, Rep, Cast]) Dest() actor.Dest { return r.dest }

// Call calls the server and waits for its reply. See actor.Call.
func (r Ref[Req, Rep, Cast]) Call(ctx context.Context, caller actor.Caller, req Req) (Rep, error) {
	var zero Rep
	v, err := actor.Call(ctx, caller, r.dest, req)
	if err != nil || v == nil {
		return zero, err
	}
	rep, ok := v.(Rep)
	if !ok {
		return zero, fmt.Errorf("genserver: reply of type %T, want %T", v, zero)
	}
	return rep, nil
}

// SendRequest calls the server without waiting for the reply, like
// gen_server:send_request: the reply is taken from the Pending, when
// there. See actor.Pending.
func (r Ref[Req, Rep, Cast]) SendRequest(caller actor.Caller, req Req) *actor.Pending[Rep] {
	return actor.Request[Rep](caller, r.dest, req)
}

// Cast sends msg to the server without waiting. See actor.SendCast.
func (r Ref[Req, Rep, Cast]) Cast(caller actor.Caller, msg Cast) {
	actor.SendCast(caller, r.dest, msg)
}

// Stop stops the server normally, like gen_server:stop: its Terminate
// callback runs, and Stop returns once it is dead.
func (r Ref[Req, Rep, Cast]) Stop(ctx context.Context, caller actor.Caller) error {
	return gen.Terminate(ctx, caller, r.dest, nil)
}

// CallEffect is the effect for a server to call this one without waiting,
// the outcome arriving as a actor.Response with tag.
func (r Ref[Req, Rep, Cast]) CallEffect(req Req, tag any) actor.Effect {
	return actor.SendRequest{To: r.dest, Req: req, Tag: tag}
}

// CastEffect is the effect for a server to cast msg to this one.
func (r Ref[Req, Rep, Cast]) CastEffect(msg Cast) actor.Effect {
	return actor.Cast{To: r.dest, Req: msg}
}

// Start starts b in a new process, like gen_server:start. It may be called
// from outside any process.
func Start[S, Req, Rep, Cast any](ctx context.Context, n *proc.Node, b Behaviour[S, Req, Rep, Cast], opts ...actor.Option) (Ref[Req, Rep, Cast], error) {
	pid, err := gen.Start(ctx, n, adapter[S, Req, Rep, Cast]{b}, nil, opts...)
	return NewRef[Req, Rep, Cast](pid), err
}

// StartLink starts b in a new process linked to parent, like
// gen_server:start_link.
func StartLink[S, Req, Rep, Cast any](ctx context.Context, parent *proc.Self, b Behaviour[S, Req, Rep, Cast], opts ...actor.Option) (Ref[Req, Rep, Cast], error) {
	pid, err := gen.StartLink(ctx, parent, adapter[S, Req, Rep, Cast]{b}, nil, opts...)
	return NewRef[Req, Rep, Cast](pid), err
}

// Child returns b as a child to start, the Start of a
// supervisor.ChildSpec, which gensim can simulate as well.
func Child[S, Req, Rep, Cast any](b Behaviour[S, Req, Rep, Cast], opts ...actor.Option) gen.Child {
	return gen.ChildOf(adapter[S, Req, Rep, Cast]{b}, nil, opts...)
}

// Gen returns b as a gen.Behaviour, the form gen and gensim run.
func Gen[S, Req, Rep, Cast any](b Behaviour[S, Req, Rep, Cast]) gen.Behaviour[S] {
	return adapter[S, Req, Rep, Cast]{b}
}

// adapter turns a Behaviour into a gen.Behaviour.
type adapter[S, Req, Rep, Cast any] struct {
	b Behaviour[S, Req, Rep, Cast]
}

// WithClock gives the clock to the behaviour, if it is actor.Clocked.
func (a adapter[S, Req, Rep, Cast]) WithClock(now func() time.Time) any {
	c, ok := a.b.(actor.Clocked)
	if !ok {
		return a
	}
	b, ok := c.WithClock(now).(Behaviour[S, Req, Rep, Cast])
	if !ok {
		return nil // the runtime fails the start
	}
	return adapter[S, Req, Rep, Cast]{b}
}

func (a adapter[S, Req, Rep, Cast]) Init(self proc.PID, _ any) (S, []actor.Effect, error) {
	return a.b.Init(self)
}

func (a adapter[S, Req, Rep, Cast]) Handle(s S, msg gen.Msg) (S, []actor.Effect) {
	switch m := msg.(type) {
	case actor.CallMsg:
		req, ok := m.Req.(Req)
		if !ok {
			return s, actor.Do(actor.Stop{Reason: &BadMessageError{Msg: m.Req}})
		}
		return a.b.HandleCall(s, req, From[Rep]{m.From})
	case actor.CastMsg:
		c, ok := m.Req.(Cast)
		if !ok {
			return s, actor.Do(actor.Stop{Reason: &BadMessageError{Msg: m.Req}})
		}
		return a.b.HandleCast(s, c)
	case gen.InfoMsg:
		if h, ok := a.b.(InfoHandler[S]); ok {
			return h.HandleInfo(s, m.Msg)
		}
	case gen.ContinueMsg:
		if h, ok := a.b.(ContinueHandler[S]); ok {
			return h.HandleContinue(s, m.Msg)
		}
		return s, actor.Do(actor.Stop{Reason: ErrNoHandleContinue})
	}
	return s, nil
}

// Label tells what the process is: the genserver and its behaviour.
func (a adapter[S, Req, Rep, Cast]) Label() string { return fmt.Sprintf("genserver %T", a.b) }

// FormatStatus formats the report of the server terminating with the
// FormatStatus of the behaviour, if it has one: see actor.StatusFormatter.
// The State is the state of the behaviour; the Message, the gen.Msg it
// was handling.
func (a adapter[S, Req, Rep, Cast]) FormatStatus(st actor.Status) actor.Status {
	if f, ok := a.b.(actor.StatusFormatter); ok {
		return f.FormatStatus(st)
	}
	return st
}

func (a adapter[S, Req, Rep, Cast]) Terminate(s S, reason error) []actor.Effect {
	if t, ok := a.b.(Terminator[S]); ok {
		return t.Terminate(s, reason)
	}
	return nil
}
