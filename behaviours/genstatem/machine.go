package genstatem

import (
	"fmt"
	"maps"
	"slices"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Machine is the state of a state machine process, as gen and gensim see
// it: the state and data of the behaviour, with what the adapter keeps.
type Machine[St comparable, D any] struct {
	state St
	data  D

	postponed  []Event // to retry after the next state change
	stateTimer bool    // a state timeout is running, with stateMsg
	eventTimer bool    // an event timeout is running, with eventMsg
	stateMsg   any
	eventMsg   any
	generic    map[any]any // the generic timeouts running, by name, with their Msg
}

// State returns the state of the machine.
func (m Machine[St, D]) State() St { return m.state }

// Data returns the data of the machine.
func (m Machine[St, D]) Data() D { return m.data }

// Timer keys, and the message of a timer firing.
type (
	stateTimerKey   struct{}
	eventTimerKey   struct{}
	genericTimerKey struct{ name any }

	// fired is a timer firing; its Msg is the machine's, as updated.
	fired struct{ key any }

	// initEvents are the events Init inserted, handled once started.
	initEvents struct{ events []Event }
)

// adapter runs a Behaviour as a gen.Behaviour.
type adapter[St comparable, D any] struct {
	b     Behaviour[St, D]
	enter bool
}

func newAdapter[St comparable, D any](b Behaviour[St, D]) adapter[St, D] {
	e, ok := b.(StateEnterer)
	return adapter[St, D]{b: b, enter: ok && e.StateEnter()}
}

func (a adapter[St, D]) Init(self proc.PID, _ any) (Machine[St, D], []molecule.Effect, error) {
	state, data, effs, err := a.b.Init(self)
	if err != nil {
		return Machine[St, D]{}, nil, err
	}
	m := Machine[St, D]{state: state, data: data}
	var out []molecule.Effect
	next, postpone, _ := a.actions(&m, effs, &out)
	if postpone {
		return m, append(out, molecule.Stop{Reason: ErrInitPostpone}), nil
	}
	if a.enter {
		if m, out = a.enterState(m, state, out); stops(out) {
			return m, out, nil
		}
	}
	if len(next) > 0 {
		out = append(out, molecule.Continue{Msg: initEvents{next}})
	}
	return m, out, nil
}

func (a adapter[St, D]) Handle(m Machine[St, D], msg gen.Msg) (Machine[St, D], []molecule.Effect) {
	var ev Event
	switch x := msg.(type) {
	case molecule.CallMsg:
		ev = Call{From: x.From, Req: x.Req}
	case molecule.CastMsg:
		ev = Cast{Msg: x.Req}
	case gen.ContinueMsg:
		if ie, ok := x.Msg.(initEvents); ok {
			return a.run(m, ie.events)
		}
		ev = Internal{Msg: x.Msg}
	case gen.InfoMsg:
		f, ok := x.Msg.(fired)
		if !ok {
			ev = Info{Msg: x.Msg}
			break
		}
		switch k := f.key.(type) {
		case stateTimerKey:
			ev = StateTimeout{Msg: m.stateMsg}
			m.stateTimer, m.stateMsg = false, nil
		case eventTimerKey:
			ev = EventTimeout{Msg: m.eventMsg}
			m.eventTimer, m.eventMsg = false, nil
		case genericTimerKey:
			ev = Timeout{Name: k.name, Msg: m.generic[k.name]}
			m.generic = without(m.generic, k.name)
		}
	}
	return a.run(m, []Event{ev})
}

// Label tells what the process is: the genstatem and its behaviour.
func (a adapter[St, D]) Label() string { return fmt.Sprintf("genstatem %T", a.b) }

// FormatStatus formats the report of the machine terminating with the
// FormatStatus of the behaviour, if it has one: see molecule.StatusFormatter.
// The State is the Machine, with its state and data.
func (a adapter[St, D]) FormatStatus(st molecule.Status) molecule.Status {
	if f, ok := a.b.(molecule.StatusFormatter); ok {
		return f.FormatStatus(st)
	}
	return st
}

func (a adapter[St, D]) Terminate(m Machine[St, D], reason error) []molecule.Effect {
	t, ok := a.b.(Terminator[St, D])
	if !ok {
		return nil
	}
	return slices.DeleteFunc(t.Terminate(m.state, m.data, reason), isAction)
}

// run handles the events in order, with those they insert or retry, until
// none is left or the machine stops.
func (a adapter[St, D]) run(m Machine[St, D], queue []Event) (Machine[St, D], []molecule.Effect) {
	var out []molecule.Effect
	for len(queue) > 0 {
		ev := queue[0]
		queue = queue[1:]
		if m.eventTimer { // any event cancels the event timeout
			out = append(out, molecule.CancelTimer{Key: eventTimerKey{}})
			m.eventTimer, m.eventMsg = false, nil
		}

		old := m.state
		state, data, effs := a.b.HandleEvent(m.state, m.data, ev)
		m.state, m.data = state, data
		changed := state != old
		if changed && m.stateTimer {
			out = append(out, molecule.CancelTimer{Key: stateTimerKey{}})
			m.stateTimer, m.stateMsg = false, nil
		}
		next, postpone, repeat := a.actions(&m, effs, &out)
		if postpone {
			m.postponed = append(slices.Clip(m.postponed), ev)
		}
		if stops(out) {
			return m, out
		}
		if !changed {
			// Repeating the state runs its enter call again; the postponed
			// events wait for a change still.
			if repeat && a.enter {
				if m, out = a.enterState(m, m.state, out); stops(out) {
					return m, out
				}
			}
			queue = slices.Concat(next, queue)
			continue
		}
		if a.enter {
			if m, out = a.enterState(m, old, out); stops(out) {
				return m, out
			}
		}
		// Inserted events first, then the postponed ones, then the rest.
		queue = slices.Concat(next, m.postponed, queue)
		m.postponed = nil
	}
	return m, out
}

// enterState handles the state enter call after a change from old.
func (a adapter[St, D]) enterState(m Machine[St, D], old St, out []molecule.Effect) (Machine[St, D], []molecule.Effect) {
	state, data, effs := a.b.HandleEvent(m.state, m.data, Enter[St]{Old: old})
	if state != m.state {
		return m, append(out, molecule.Stop{Reason: ErrEnterChangedState})
	}
	m.data = data
	if next, postpone, repeat := a.actions(&m, effs, &out); postpone || repeat || len(next) > 0 {
		return m, append(out, molecule.Stop{Reason: ErrEnterAction})
	}
	return m, out
}

// actions performs the actions among effs on m, turning timeouts into gen
// timers, and appends the other effects to out. It returns the events to
// insert, whether to postpone the event, and whether to repeat the state.
func (a adapter[St, D]) actions(m *Machine[St, D], effs []molecule.Effect, out *[]molecule.Effect) (next []Event, postpone, repeat bool) {
	for _, e := range effs {
		switch x := e.(type) {
		case Postpone:
			postpone = true
		case NextEvent:
			next = append(next, x.Event)
		case RepeatState:
			repeat = true
		case StartStateTimeout:
			*out = append(*out, molecule.StartTimer{Key: stateTimerKey{}, After: x.After, At: x.At, Msg: fired{stateTimerKey{}}})
			m.stateTimer, m.stateMsg = true, x.Msg
		case CancelStateTimeout:
			if m.stateTimer {
				*out = append(*out, molecule.CancelTimer{Key: stateTimerKey{}})
				m.stateTimer, m.stateMsg = false, nil
			}
		case UpdateStateTimeout:
			if m.stateTimer {
				m.stateMsg = x.Msg
			} else {
				next = append(next, StateTimeout{Msg: x.Msg})
			}
		case StartEventTimeout:
			*out = append(*out, molecule.StartTimer{Key: eventTimerKey{}, After: x.After, At: x.At, Msg: fired{eventTimerKey{}}})
			m.eventTimer, m.eventMsg = true, x.Msg
		case UpdateEventTimeout:
			if m.eventTimer {
				m.eventMsg = x.Msg
			} else {
				next = append(next, EventTimeout{Msg: x.Msg})
			}
		case StartTimeout:
			key := genericTimerKey{x.Name}
			*out = append(*out, molecule.StartTimer{Key: key, After: x.After, At: x.At, Msg: fired{key}})
			m.generic = with(m.generic, x.Name, x.Msg)
		case UpdateTimeout:
			if _, ok := m.generic[x.Name]; ok {
				m.generic = with(m.generic, x.Name, x.Msg)
			} else {
				next = append(next, Timeout{Name: x.Name, Msg: x.Msg})
			}
		case CancelTimeout:
			if _, ok := m.generic[x.Name]; ok {
				*out = append(*out, molecule.CancelTimer{Key: genericTimerKey{x.Name}})
				m.generic = without(m.generic, x.Name)
			}
		default:
			*out = append(*out, e)
		}
	}
	return next, postpone, repeat
}

// with and without return a copy of the map m, changed: the machine is a
// value, never changed in place.
func with(m map[any]any, k, v any) map[any]any {
	c := maps.Clone(m)
	if c == nil {
		c = make(map[any]any)
	}
	c[k] = v
	return c
}

func without(m map[any]any, k any) map[any]any {
	c := maps.Clone(m)
	delete(c, k)
	return c
}

func isAction(e molecule.Effect) bool {
	switch e.(type) {
	case Postpone, NextEvent, RepeatState,
		StartStateTimeout, CancelStateTimeout, UpdateStateTimeout,
		StartEventTimeout, UpdateEventTimeout,
		StartTimeout, UpdateTimeout, CancelTimeout:
		return true
	}
	return false
}

func stops(out []molecule.Effect) bool {
	return slices.ContainsFunc(out, func(e molecule.Effect) bool {
		_, ok := e.(molecule.Stop)
		return ok
	})
}
