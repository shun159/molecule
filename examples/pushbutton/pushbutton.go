package main

// This file is the pushbutton module of the gen_statem documentation,
// laid out as pushbutton.erl is: the name, the API, the callbacks, then a
// function per state, as callback_mode state_functions has.
//
// In Go, a state is a type: off and on each have a handleEvent method,
// like the off/3 and on/3 functions, and the machine's HandleEvent hands
// each event to the method of the current state.

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// name is the registered server name, and button the machine there, to
// call with its requests.
var (
	name   = actor.Local("pushbutton_statem")
	button = actor.NewRef[Pushbutton](name)
)

//
// API. This example uses a registered name and does not link to the
// caller.
//

func start(ctx context.Context, n *proc.Node) (proc.PID, error) {
	return genstatem.Start(ctx, n, Pushbutton{}, actor.WithName(name))
}

func push(ctx context.Context, n *proc.Node) (string, error) {
	return button.Call(ctx, n, pushReq{})
}

func getCount(ctx context.Context, n *proc.Node) (int, error) {
	return button.Call(ctx, n, getCountReq{})
}

func stop(ctx context.Context, n *proc.Node) error {
	return genstatem.Stop(ctx, n, name)
}

// The requests, the atoms push and get_count, each with the type of its
// reply.
type (
	pushReq     struct{ actor.Req[Pushbutton, string] }
	getCountReq struct{ actor.Req[Pushbutton, int] }
)

//
// Callbacks. terminate and code_change are optional here: there is
// nothing to clean up, and no hot code upgrade in Go.
//

// Pushbutton is the state machine. Its data is a counter of pushes.
type Pushbutton struct{}

// State is a state of the button: a type handling the events in that
// state.
type State interface {
	handleEvent(ev genstatem.Event, data int) (State, int, []actor.Effect)
}

// Init sets the initial state and data.
func (Pushbutton) Init(proc.PID) (State, int, []actor.Effect, error) {
	return off{}, 0, nil, nil
}

// HandleEvent hands the event to the current state, as state_functions
// calls the function named by the state.
func (Pushbutton) HandleEvent(state State, data int, ev genstatem.Event) (State, int, []actor.Effect) {
	return state.handleEvent(ev, data)
}

//
// State callbacks.
//

type (
	off struct{}
	on  struct{}
)

func (off) handleEvent(ev genstatem.Event, data int) (State, int, []actor.Effect) {
	if call, r, ok := callOf[pushReq](ev); ok {
		// Go to on, increment the count and reply that the resulting
		// status is on.
		return on{}, data + 1, actor.Do(r.Reply(call.From, "on"))
	}
	return handleEvent(off{}, ev, data)
}

func (on) handleEvent(ev genstatem.Event, data int) (State, int, []actor.Effect) {
	if call, r, ok := callOf[pushReq](ev); ok {
		// Go to off and reply that the resulting status is off.
		return off{}, data, actor.Do(r.Reply(call.From, "off"))
	}
	return handleEvent(on{}, ev, data)
}

// handleEvent handles the events common to all states.
func handleEvent(state State, ev genstatem.Event, data int) (State, int, []actor.Effect) {
	if call, r, ok := callOf[getCountReq](ev); ok {
		// Reply with the current count.
		return state, data, actor.Do(r.Reply(call.From, data))
	}
	// Ignore all other events.
	return state, data, nil
}

// callOf returns ev as a call of a request R, and its request, if it is
// one: as a clause matching {call, From}, push.
func callOf[R any](ev genstatem.Event) (genstatem.Call, R, bool) {
	call, isCall := ev.(genstatem.Call)
	r, isR := call.Req.(R)
	return call, r, isCall && isR
}
