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

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// name is the registered server name.
var name = molecule.Local("pushbutton_statem")

//
// API. This example uses a registered name and does not link to the
// caller.
//

func start(ctx context.Context, n *proc.Node) (proc.PID, error) {
	return genstatem.Start(ctx, n, Pushbutton{}, molecule.WithName(name))
}

func push(ctx context.Context, n *proc.Node) (any, error) {
	return genstatem.NewRef(name).Call(ctx, n, pushReq{})
}

func getCount(ctx context.Context, n *proc.Node) (any, error) {
	return genstatem.NewRef(name).Call(ctx, n, getCountReq{})
}

func stop(ctx context.Context, n *proc.Node) error {
	return genstatem.Stop(ctx, n, name)
}

// The requests, the atoms push and get_count.
type (
	pushReq     struct{}
	getCountReq struct{}
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
	handleEvent(ev genstatem.Event, data int) (State, int, []molecule.Effect)
}

// Init sets the initial state and data.
func (Pushbutton) Init(proc.PID) (State, int, []molecule.Effect, error) {
	return off{}, 0, nil, nil
}

// HandleEvent hands the event to the current state, as state_functions
// calls the function named by the state.
func (Pushbutton) HandleEvent(state State, data int, ev genstatem.Event) (State, int, []molecule.Effect) {
	return state.handleEvent(ev, data)
}

//
// State callbacks.
//

type (
	off struct{}
	on  struct{}
)

func (off) handleEvent(ev genstatem.Event, data int) (State, int, []molecule.Effect) {
	if call, ok := ev.(genstatem.Call); ok && call.Req == (pushReq{}) {
		// Go to on, increment the count and reply that the resulting
		// status is on.
		return on{}, data + 1, molecule.Do(call.Reply("on"))
	}
	return handleEvent(off{}, ev, data)
}

func (on) handleEvent(ev genstatem.Event, data int) (State, int, []molecule.Effect) {
	if call, ok := ev.(genstatem.Call); ok && call.Req == (pushReq{}) {
		// Go to off and reply that the resulting status is off.
		return off{}, data, molecule.Do(call.Reply("off"))
	}
	return handleEvent(on{}, ev, data)
}

// handleEvent handles the events common to all states.
func handleEvent(state State, ev genstatem.Event, data int) (State, int, []molecule.Effect) {
	if call, ok := ev.(genstatem.Call); ok && call.Req == (getCountReq{}) {
		// Reply with the current count.
		return state, data, molecule.Do(call.Reply(data))
	}
	// Ignore all other events.
	return state, data, nil
}
