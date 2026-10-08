// Package genstatem is the generic state machine behaviour, like Erlang's
// gen_statem in the handle_event_function callback mode.
//
// A state machine is a type implementing [Behaviour] with states of type
// St and data of type D. One pure callback, HandleEvent, gets every event
// with the current state and data, and returns the next ones with effects.
// The state machine actions of gen_statem are effects too, returned among
// the others.
//
// # Callbacks
//
//	Init(self) (St, D, []gen.Effect, error)          init/1
//	HandleEvent(St, D, Event) (St, D, []gen.Effect)  handle_event/4
//	StateEnter() bool                                state_enter callback mode, optional
//	Terminate(St, D, error) []gen.Effect             terminate/3, optional
//
// # States
//
// Returning a state different from the current one, by ==, is a state
// change; returning the same state keeps it. States must be comparable,
// also at run time: a state of an interface type holding a slice panics
// when compared.
//
// The state_functions callback mode, where each state has a function of
// its own, is written with a type per state. The states are values of an
// interface with a method handling the events, and HandleEvent hands each
// event to the current state:
//
//	type State interface {
//		handleEvent(ev genstatem.Event, data int) (State, int, []gen.Effect)
//	}
//
//	func (Pushbutton) HandleEvent(s State, data int, ev genstatem.Event) (State, int, []gen.Effect) {
//		return s.handleEvent(ev, data)
//	}
//
// See examples/pushbutton.
//
// # Events
//
//	Call          {call, From}; answer with its Reply method
//	Cast          cast
//	Info          info
//	Enter         a state enter call, with the state left
//	StateTimeout  state_timeout
//	EventTimeout  timeout
//	Timeout       {timeout, Name}
//	Internal      internal, from a NextEvent
//
// # Actions
//
//	Postpone            postpone
//	NextEvent           {next_event, Type, Content}
//	RepeatState         repeat_state: the state enter call again
//	StartStateTimeout   {state_timeout, Time, Msg}, At for {abs, true}
//	UpdateStateTimeout  {state_timeout, update, Msg}
//	CancelStateTimeout  {state_timeout, cancel}
//	StartEventTimeout   {timeout, Time, Msg}
//	UpdateEventTimeout  {timeout, update, Msg}
//	StartTimeout        {{timeout, Name}, Time, Msg}
//	UpdateTimeout       {{timeout, Name}, update, Msg}
//	CancelTimeout       {{timeout, Name}, cancel}
//
// A postponed event is handled again after the next state change. An event
// inserted with NextEvent is handled next, before anything in the mailbox;
// several are handled in the order returned.
//
// On a state change, the state timeout is cancelled, then the actions of the
// transition run; a state timeout they start is one of the new state. The
// events then come in this order: the state enter call, the inserted
// events, the postponed events, and the mailbox.
//
// An event timeout is cancelled by any event. A generic timeout runs until
// it fires or is cancelled, through state changes; starting one under a
// name already running replaces it.
//
// # State enter calls
//
// A machine whose StateEnter returns true gets an [Enter] event on every
// state change, and for its initial state, with the state itself as Old.
// An Enter may change the data and start or cancel timeouts. Changing the
// state stops the machine with [ErrEnterChangedState]; postponing or
// inserting events stops it with [ErrEnterAction].
//
// # Starting and stopping
//
// [Start] and [StartLink] start a machine and wait until Init has run. The
// events Init inserts are handled after the start has returned; if one of
// them stops the machine, the start has still succeeded. [Stop] stops a
// machine normally, and [GetState] returns its state and data.
//
// # Clients
//
// A [Ref] calls and casts a machine, at a PID or a name, like
// gen_statem:call, gen_statem:cast and gen_statem:send_request:
//
//	lock := genstatem.NewRef(gen.Local("lock"))
//	lock.Cast(caller, Button{1})
//	status, err := lock.Call(ctx, caller, Status{})
//	p := lock.SendRequest(caller, Status{}) // the reply, from p, later
//
// A machine implementing gen.StatusFormatter formats what the report of
// its terminating tells, like format_status/1; its State is the Machine.
//
// # Differences from gen_statem
//
// There is no hibernation, no code change, and no change of callback
// module. States are not atoms naming functions: state_functions is
// written with types, as above.
package genstatem
