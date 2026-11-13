package main

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
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
	// Status asks for the state, a call replied the name of the state.
	Status struct{ actor.Req[CodeLock, string] }
)

// clearTime is how long a partial code is kept without a press.
const clearTime = 5 * time.Second

// Timeout messages.
type (
	lock  struct{}
	clear struct{}
)

func (CodeLock) Init(proc.PID) (State, Data, []actor.Effect, error) {
	return Locked, Data{}, nil, nil
}

// StateEnter asks for state enter calls: entering a state locks or
// unlocks the door.
func (CodeLock) StateEnter() bool { return true }

// HandleEvent handles what comes in any state, then hands the rest to the
// function of the state, as state_functions has locked/3 and open/3.
func (l CodeLock) HandleEvent(state State, data Data, ev genstatem.Event) (State, Data, []actor.Effect) {
	switch e := ev.(type) {
	case genstatem.Enter[State]:
		return l.entered(state, data)
	case genstatem.Call:
		return state, data, status(state, e)
	}
	if state == Open {
		return l.open(data, ev)
	}
	return l.locked(data, ev)
}

// entered locks or unlocks the door: locked, the buttons pressed are
// forgotten; open, it locks again after OpenTime.
func (l CodeLock) entered(state State, data Data) (State, Data, []actor.Effect) {
	if state == Locked {
		return state, Data{}, actor.Do(l.show("door locked"))
	}
	return state, data, actor.Do(l.show("door open"),
		genstatem.StartStateTimeout{After: l.OpenTime, Msg: lock{}})
}

// status replies to a Status call with the state, in any state.
func status(state State, c genstatem.Call) []actor.Effect {
	if r, ok := c.Req.(Status); ok {
		return actor.Do(r.Reply(c.From, state.String()))
	}
	return actor.Do(actor.Unknown(c.From, c.Req))
}

// locked takes the buttons pressed, and forgets them after clearTime
// without a press.
func (l CodeLock) locked(data Data, ev genstatem.Event) (State, Data, []actor.Effect) {
	switch e := ev.(type) {
	case genstatem.Cast:
		if b, ok := e.Msg.(Button); ok {
			return l.pressed(data, b.N)
		}
	case genstatem.EventTimeout: // clear
		return Locked, Data{}, actor.Do(l.show("code cleared"))
	}
	return Locked, data, nil
}

// pressed adds the button n to those pressed: the code opens the door, and
// a wrong code, once as long, starts again.
func (l CodeLock) pressed(data Data, n int) (State, Data, []actor.Effect) {
	buttons := append(slices.Clip(data.Buttons), n)
	switch {
	case slices.Equal(buttons, l.Code):
		return Open, Data{}, actor.Do(l.show("correct code"))
	case len(buttons) == len(l.Code):
		return Locked, Data{}, actor.Do(l.show("wrong code"))
	}
	return Locked, Data{Buttons: buttons},
		actor.Do(genstatem.StartEventTimeout{After: clearTime, Msg: clear{}})
}

// open has the buttons wait until the door locks again, at the state
// timeout.
func (l CodeLock) open(data Data, ev genstatem.Event) (State, Data, []actor.Effect) {
	switch ev.(type) {
	case genstatem.Cast:
		return Open, data, actor.Do(genstatem.Postpone{})
	case genstatem.StateTimeout: // lock
		return Locked, data, nil
	}
	return Open, data, nil
}

func (l CodeLock) show(text string) actor.Effect {
	return actor.Send{To: l.Display, Msg: text}
}
