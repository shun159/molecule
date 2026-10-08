package genstatem

import (
	"context"
	"errors"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// Behaviour is a state machine with states St and data D.
//
// A Behaviour may also implement StateEnterer and Terminator.
type Behaviour[St comparable, D any] interface {
	// Init returns the initial state and data. self is the PID of the
	// machine. Its effects may hold actions: timeouts to start, events to
	// insert, handled once Start has returned.
	Init(self proc.PID) (St, D, []gen.Effect, error)
	// HandleEvent handles one event, and returns the next state and data.
	// A state different from state, by ==, is a state change: the state
	// timeout is cancelled, postponed events are retried, and with state
	// enter calls, Enter is handled first.
	HandleEvent(state St, data D, ev Event) (St, D, []gen.Effect)
}

// StateEnterer is implemented by machines wanting state enter calls, like
// the state_enter callback mode: on every state change, HandleEvent gets
// Enter, and also for the initial state, with Old the state itself. An
// Enter may change the data and start timeouts, but neither change the
// state, postpone, nor insert events, or the machine stops.
type StateEnterer interface {
	StateEnter() bool
}

// Terminator is called when the machine stops, see gen.Behaviour. Actions
// it returns are ignored.
type Terminator[St comparable, D any] interface {
	Terminate(state St, data D, reason error) []gen.Effect
}

// Errors stopping a machine misusing actions.
var (
	ErrEnterChangedState = errors.New("genstatem: state changed in a state enter call")
	ErrEnterAction       = errors.New("genstatem: postpone or next event in a state enter call")
	ErrInitPostpone      = errors.New("genstatem: postpone in Init")
)

// Event is what HandleEvent gets: Call, Cast, Info, Enter, StateTimeout,
// EventTimeout, Timeout, or Internal.
type Event interface{ event() }

type (
	// Call is a request from gen.Call, to answer with Reply.
	Call struct {
		From gen.From
		Req  any
	}
	// Cast is a request from gen.SendCast.
	Cast struct{ Msg any }
	// Info is any other message.
	Info struct{ Msg any }
	// Enter is a state enter call: the state just changed from Old.
	Enter[St comparable] struct{ Old St }
	// StateTimeout is the Msg of a StartStateTimeout, no state change
	// having happened meanwhile.
	StateTimeout struct{ Msg any }
	// EventTimeout is the Msg of a StartEventTimeout, no event having
	// arrived meanwhile.
	EventTimeout struct{ Msg any }
	// Timeout is the Msg of the generic timeout Name.
	Timeout struct {
		Name any
		Msg  any
	}
	// Internal is an event inserted with NextEvent, or the Msg of a
	// gen.Continue.
	Internal struct{ Msg any }
)

func (Call) event()         {}
func (Cast) event()         {}
func (Info) event()         {}
func (Enter[St]) event()    {}
func (StateTimeout) event() {}
func (EventTimeout) event() {}
func (Timeout) event()      {}
func (Internal) event()     {}

// Reply is the effect answering the call.
func (c Call) Reply(v any) gen.Effect { return gen.Reply{To: c.From, Value: v} }

// Actions, the transition actions of gen_statem. They are effects, to
// return among the others.
type (
	// Postpone keeps the event to handle again after the next state
	// change.
	Postpone struct{ gen.Extension }
	// NextEvent inserts Event to handle next, before anything in the
	// mailbox. Several are handled in the order returned.
	NextEvent struct {
		gen.Extension
		Event Event
	}
	// StartStateTimeout makes StateTimeout{Msg} arrive after After,
	// unless the state changes first. It replaces a running one.
	StartStateTimeout struct {
		gen.Extension
		After time.Duration
		Msg   any
	}
	// CancelStateTimeout cancels the state timeout.
	CancelStateTimeout struct{ gen.Extension }
	// StartEventTimeout makes EventTimeout{Msg} arrive after After,
	// unless another event arrives first.
	StartEventTimeout struct {
		gen.Extension
		After time.Duration
		Msg   any
	}
	// StartTimeout makes Timeout{Name, Msg} arrive after After, whatever
	// happens meanwhile. It replaces a running one of the same Name.
	StartTimeout struct {
		gen.Extension
		Name  any
		After time.Duration
		Msg   any
	}
	// CancelTimeout cancels the generic timeout Name.
	CancelTimeout struct {
		gen.Extension
		Name any
	}
)

// Gen returns b as a gen.Behaviour, the form gen and gensim run. Its state
// is opaque; GetState and gensim give the state and data of the machine.
func Gen[St comparable, D any](b Behaviour[St, D]) gen.Behaviour[Machine[St, D]] {
	return newAdapter(b)
}

// Start starts b in a new process, like gen_statem:start.
func Start[St comparable, D any](ctx context.Context, n *proc.Node, b Behaviour[St, D], opts ...gen.Option) (proc.PID, error) {
	return gen.Start(ctx, n, newAdapter(b), nil, opts...)
}

// StartLink starts b in a new process linked to parent, like
// gen_statem:start_link.
func StartLink[St comparable, D any](ctx context.Context, parent *proc.Self, b Behaviour[St, D], opts ...gen.Option) (proc.PID, error) {
	return gen.StartLink(ctx, parent, newAdapter(b), nil, opts...)
}

// StartLinkFunc returns a function that starts b linked to its parent, to
// use as the Start of a supervisor.ChildSpec.
func StartLinkFunc[St comparable, D any](b Behaviour[St, D], opts ...gen.Option) func(context.Context, *proc.Self) (proc.PID, error) {
	return gen.StartLinkFunc(newAdapter(b), nil, opts...)
}

// Ref is a handle on a machine, to call and cast it.
type Ref struct{ dest gen.Dest }

// NewRef makes a Ref to the machine at dest, a PID or a name.
func NewRef(dest gen.Dest) Ref { return Ref{dest} }

// Dest returns where the machine is.
func (r Ref) Dest() gen.Dest { return r.dest }

// Call sends req to the machine, as a Call event, and waits for the
// reply, like gen_statem:call. See gen.Call.
func (r Ref) Call(ctx context.Context, caller gen.Caller, req any) (any, error) {
	return gen.Call(ctx, caller, r.dest, req)
}

// Cast sends msg to the machine, as a Cast event, without waiting, like
// gen_statem:cast. It never fails.
func (r Ref) Cast(caller gen.Caller, msg any) { gen.SendCast(caller, r.dest, msg) }

// SendRequest calls the machine without waiting for the reply, like
// gen_statem:send_request. See gen.Pending.
func (r Ref) SendRequest(caller gen.Caller, req any) *gen.Pending[any] {
	return gen.Request[any](caller, r.dest, req)
}

// Stop stops the machine at to normally, like gen_statem:stop: its
// Terminate callback runs, and Stop returns once it is dead.
func Stop(ctx context.Context, caller gen.Caller, to gen.Dest) error {
	return gen.Terminate(ctx, caller, to, nil)
}

// GetState returns the state and data of the machine at to, like
// sys:get_state.
func GetState[St comparable, D any](ctx context.Context, caller gen.Caller, to gen.Dest) (St, D, error) {
	v, err := gen.GetState(ctx, caller, to)
	m, ok := v.(Machine[St, D])
	if err == nil && !ok {
		err = errors.New("genstatem: not a state machine of these types")
	}
	return m.state, m.data, err
}
