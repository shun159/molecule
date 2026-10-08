package genserver

import (
	"context"
	"errors"
	"fmt"

	"github.com/shun159/molecule"
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
	Init(self proc.PID) (S, []molecule.Effect, error)
	// HandleCall handles a call. The reply is an effect made with
	// from.Reply, now or, keeping from in the state, later.
	HandleCall(state S, req Req, from From[Rep]) (S, []molecule.Effect)
	HandleCast(state S, msg Cast) (S, []molecule.Effect)
}

// Default gives a Behaviour the callbacks it has no use for, embedded in
// it: Init starts with the zero state, and a server without HandleCall or
// HandleCast takes no calls or no casts, one stopping it with a
// BadMessageError, as a request of the wrong type does.
//
//	// Log keeps what it is sent.
//	type Log struct{ genserver.Default[[]string] }
//
//	func (Log) HandleInfo(log []string, msg any) ([]string, []molecule.Effect) {
//		return append(slices.Clip(log), fmt.Sprint(msg)), nil
//	}
type Default[S any] struct{}

// None is the request, reply or cast type of a server taking none, by
// Default: no value has it but nil, which no request carries.
type None interface{ none() }

func (Default[S]) Init(proc.PID) (S, []molecule.Effect, error) {
	var zero S
	return zero, nil, nil
}

func (Default[S]) HandleCall(s S, _ None, _ From[None]) (S, []molecule.Effect) { return s, nil }

func (Default[S]) HandleCast(s S, _ None) (S, []molecule.Effect) { return s, nil }

// InfoHandler handles the messages that are neither calls nor casts, see
// gen.InfoMsg. Without it, they are dropped.
type InfoHandler[S any] interface {
	HandleInfo(state S, msg any) (S, []molecule.Effect)
}

// ContinueHandler handles the Msg of a molecule.Continue effect, right after
// the callback that returned it, like handle_continue in OTP. A server
// returning molecule.Continue must implement it, or stops with
// ErrNoHandleContinue.
type ContinueHandler[S any] interface {
	HandleContinue(state S, msg any) (S, []molecule.Effect)
}

// ErrNoHandleContinue is the exit reason of a server returning molecule.Continue
// without implementing ContinueHandler.
var ErrNoHandleContinue = errors.New("genserver: molecule.Continue without HandleContinue")

// Terminator is called when the server stops, see gen.Behaviour.
type Terminator[S any] interface {
	Terminate(state S, reason error) []molecule.Effect
}

// From identifies a call awaiting a reply of type Rep.
type From[Rep any] struct {
	molecule.From
}

// Reply returns the effect that replies v to the call.
func (f From[Rep]) Reply(v Rep) molecule.Effect {
	return molecule.Reply{To: f.From, Value: v}
}

// BadMessageError is the exit reason of a server that got a call or cast
// of the wrong type, which only raw molecule.Call or molecule.SendCast can send.
type BadMessageError struct {
	Msg any
}

func (e *BadMessageError) Error() string {
	return fmt.Sprintf("genserver: unexpected %T", e.Msg)
}

// Ref is a typed handle on a server.
type Ref[Req, Rep, Cast any] struct {
	dest molecule.Dest
}

// NewRef makes a Ref to the server at dest, such as a molecule.Local name.
func NewRef[Req, Rep, Cast any](dest molecule.Dest) Ref[Req, Rep, Cast] {
	return Ref[Req, Rep, Cast]{dest: dest}
}

// RefFor makes a Ref to the server running b at dest, taking its types
// from b: genserver.RefFor(Counter{}, molecule.Local("counter")).
func RefFor[S, Req, Rep, Cast any](_ Behaviour[S, Req, Rep, Cast], dest molecule.Dest) Ref[Req, Rep, Cast] {
	return NewRef[Req, Rep, Cast](dest)
}

// Dest returns where the server is.
func (r Ref[Req, Rep, Cast]) Dest() molecule.Dest { return r.dest }

// Call calls the server and waits for its reply. See molecule.Call.
func (r Ref[Req, Rep, Cast]) Call(ctx context.Context, caller molecule.Caller, req Req) (Rep, error) {
	var zero Rep
	v, err := molecule.Call(ctx, caller, r.dest, req)
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
// there. See molecule.Pending.
func (r Ref[Req, Rep, Cast]) SendRequest(caller molecule.Caller, req Req) *molecule.Pending[Rep] {
	return molecule.Request[Rep](caller, r.dest, req)
}

// Cast sends msg to the server without waiting. See molecule.SendCast.
func (r Ref[Req, Rep, Cast]) Cast(caller molecule.Caller, msg Cast) {
	molecule.SendCast(caller, r.dest, msg)
}

// Stop stops the server normally, like gen_server:stop: its Terminate
// callback runs, and Stop returns once it is dead.
func (r Ref[Req, Rep, Cast]) Stop(ctx context.Context, caller molecule.Caller) error {
	return gen.Terminate(ctx, caller, r.dest, nil)
}

// CallEffect is the effect for a server to call this one without waiting,
// the outcome arriving as a molecule.Response with tag.
func (r Ref[Req, Rep, Cast]) CallEffect(req Req, tag any) molecule.Effect {
	return molecule.SendRequest{To: r.dest, Req: req, Tag: tag}
}

// CastEffect is the effect for a server to cast msg to this one.
func (r Ref[Req, Rep, Cast]) CastEffect(msg Cast) molecule.Effect {
	return molecule.Cast{To: r.dest, Req: msg}
}

// Start starts b in a new process, like gen_server:start. It may be called
// from outside any process.
func Start[S, Req, Rep, Cast any](ctx context.Context, n *proc.Node, b Behaviour[S, Req, Rep, Cast], opts ...molecule.Option) (Ref[Req, Rep, Cast], error) {
	pid, err := gen.Start(ctx, n, adapter[S, Req, Rep, Cast]{b}, nil, opts...)
	return NewRef[Req, Rep, Cast](pid), err
}

// StartLink starts b in a new process linked to parent, like
// gen_server:start_link.
func StartLink[S, Req, Rep, Cast any](ctx context.Context, parent *proc.Self, b Behaviour[S, Req, Rep, Cast], opts ...molecule.Option) (Ref[Req, Rep, Cast], error) {
	pid, err := gen.StartLink(ctx, parent, adapter[S, Req, Rep, Cast]{b}, nil, opts...)
	return NewRef[Req, Rep, Cast](pid), err
}

// Child returns b as a child to start, the Start of a
// supervisor.ChildSpec, which gensim can simulate as well.
func Child[S, Req, Rep, Cast any](b Behaviour[S, Req, Rep, Cast], opts ...molecule.Option) gen.Child {
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

func (a adapter[S, Req, Rep, Cast]) Init(self proc.PID, _ any) (S, []molecule.Effect, error) {
	return a.b.Init(self)
}

func (a adapter[S, Req, Rep, Cast]) Handle(s S, msg gen.Msg) (S, []molecule.Effect) {
	switch m := msg.(type) {
	case molecule.CallMsg:
		req, ok := m.Req.(Req)
		if !ok {
			return s, molecule.Do(molecule.Stop{Reason: &BadMessageError{Msg: m.Req}})
		}
		return a.b.HandleCall(s, req, From[Rep]{m.From})
	case molecule.CastMsg:
		c, ok := m.Req.(Cast)
		if !ok {
			return s, molecule.Do(molecule.Stop{Reason: &BadMessageError{Msg: m.Req}})
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
		return s, molecule.Do(molecule.Stop{Reason: ErrNoHandleContinue})
	}
	return s, nil
}

// FormatStatus formats the report of the server terminating with the
// FormatStatus of the behaviour, if it has one: see molecule.StatusFormatter.
// The State is the state of the behaviour; the Message, the gen.Msg it
// was handling.
func (a adapter[S, Req, Rep, Cast]) FormatStatus(st molecule.Status) molecule.Status {
	if f, ok := a.b.(molecule.StatusFormatter); ok {
		return f.FormatStatus(st)
	}
	return st
}

func (a adapter[S, Req, Rep, Cast]) Terminate(s S, reason error) []molecule.Effect {
	if t, ok := a.b.(Terminator[S]); ok {
		return t.Terminate(s, reason)
	}
	return nil
}
