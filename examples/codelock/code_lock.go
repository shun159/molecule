package main

import (
	"slices"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

// CodeLock is a gen_statem: a door, Locked until its code is pressed, then
// Open for OpenTime. It tells what happens to Display.
type CodeLock struct {
	Code     []int
	OpenTime time.Duration
	Display  proc.PID
}

// State is the state of the lock.
type State int

const (
	Locked State = iota
	Open
)

func (s State) String() string {
	if s == Open {
		return "open"
	}
	return "locked"
}

// Data is the data of the lock: the buttons pressed so far.
type Data struct {
	Buttons []int
}

type (
	// Button is a button press, a cast.
	Button struct{ N int }
	// Status asks for the state, a call.
	Status struct{}
)

// clearTime is how long a partial code is kept without a press.
const clearTime = 5 * time.Second

// Timeout messages.
type (
	lock  struct{}
	clear struct{}
)

func (CodeLock) Init(proc.PID) (State, Data, []gen.Effect, error) {
	return Locked, Data{}, nil, nil
}

// StateEnter asks for state enter calls: entering a state locks or
// unlocks the door.
func (CodeLock) StateEnter() bool { return true }

func (l CodeLock) HandleEvent(state State, data Data, ev genstatem.Event) (State, Data, []gen.Effect) {
	switch e := ev.(type) {
	case genstatem.Enter[State]:
		if state == Locked {
			return state, Data{}, gen.Do(l.show("door locked"))
		}
		return state, data, gen.Do(l.show("door open"),
			genstatem.StartStateTimeout{After: l.OpenTime, Msg: lock{}})

	case genstatem.Call: // Status, in any state
		return state, data, gen.Do(e.Reply(state.String()))

	case genstatem.StateTimeout: // lock, in Open
		return Locked, data, nil
	}

	if state == Open {
		if _, ok := ev.(genstatem.Cast); ok {
			// A button while open waits for the door to lock again.
			return state, data, gen.Do(genstatem.Postpone{})
		}
		return state, data, nil
	}

	switch e := ev.(type) {
	case genstatem.Cast:
		b, ok := e.Msg.(Button)
		if !ok {
			break
		}
		buttons := append(slices.Clip(data.Buttons), b.N)
		switch {
		case slices.Equal(buttons, l.Code):
			return Open, Data{}, gen.Do(l.show("correct code"))
		case len(buttons) == len(l.Code):
			return state, Data{}, gen.Do(l.show("wrong code"))
		}
		return state, Data{Buttons: buttons},
			gen.Do(genstatem.StartEventTimeout{After: clearTime, Msg: clear{}})
	case genstatem.EventTimeout: // clear: no press for a while
		return state, Data{}, gen.Do(l.show("code cleared"))
	}
	return state, data, nil
}

func (l CodeLock) show(text string) gen.Effect {
	return gen.Send{To: l.Display, Msg: text}
}
