package genstatem

import (
	"slices"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// Machine is the state of a state machine process, as gen and gensim see
// it: the state and data of the behaviour, with what the adapter keeps.
type Machine[St comparable, D any] struct {
	state St
	data  D

	postponed  []Event // to retry after the next state change
	stateTimer bool    // a state timeout is running
	eventTimer bool    // an event timeout is running
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

	fired struct {
		key any
		msg any
	}

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

func (a adapter[St, D]) Init(self proc.PID, _ any) (Machine[St, D], []gen.Effect, error) {
	state, data, effs, err := a.b.Init(self)
	if err != nil {
		return Machine[St, D]{}, nil, err
	}
	m := Machine[St, D]{state: state, data: data}
	var out []gen.Effect
	next, postpone := a.actions(&m, effs, &out)
	if postpone {
		return m, append(out, gen.Stop{Reason: ErrInitPostpone}), nil
	}
	if a.enter {
		if m, out = a.enterState(m, state, out); stops(out) {
			return m, out, nil
		}
	}
	if len(next) > 0 {
		out = append(out, gen.Continue{Msg: initEvents{next}})
	}
	return m, out, nil
}

func (a adapter[St, D]) Handle(m Machine[St, D], msg gen.Msg) (Machine[St, D], []gen.Effect) {
	var ev Event
	switch x := msg.(type) {
	case gen.CallMsg:
		ev = Call{From: x.From, Req: x.Req}
	case gen.CastMsg:
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
			m.stateTimer = false
			ev = StateTimeout{Msg: f.msg}
		case eventTimerKey:
			m.eventTimer = false
			ev = EventTimeout{Msg: f.msg}
		case genericTimerKey:
			ev = Timeout{Name: k.name, Msg: f.msg}
		}
	}
	return a.run(m, []Event{ev})
}

func (a adapter[St, D]) Terminate(m Machine[St, D], reason error) []gen.Effect {
	t, ok := a.b.(Terminator[St, D])
	if !ok {
		return nil
	}
	return slices.DeleteFunc(t.Terminate(m.state, m.data, reason), isAction)
}

// run handles the events in order, with those they insert or retry, until
// none is left or the machine stops.
func (a adapter[St, D]) run(m Machine[St, D], queue []Event) (Machine[St, D], []gen.Effect) {
	var out []gen.Effect
	for len(queue) > 0 {
		ev := queue[0]
		queue = queue[1:]
		if m.eventTimer { // any event cancels the event timeout
			out = append(out, gen.CancelTimer{Key: eventTimerKey{}})
			m.eventTimer = false
		}

		old := m.state
		state, data, effs := a.b.HandleEvent(m.state, m.data, ev)
		m.state, m.data = state, data
		changed := state != old
		if changed && m.stateTimer {
			out = append(out, gen.CancelTimer{Key: stateTimerKey{}})
			m.stateTimer = false
		}
		next, postpone := a.actions(&m, effs, &out)
		if postpone {
			m.postponed = append(slices.Clip(m.postponed), ev)
		}
		if stops(out) {
			return m, out
		}
		if !changed {
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
func (a adapter[St, D]) enterState(m Machine[St, D], old St, out []gen.Effect) (Machine[St, D], []gen.Effect) {
	state, data, effs := a.b.HandleEvent(m.state, m.data, Enter[St]{Old: old})
	if state != m.state {
		return m, append(out, gen.Stop{Reason: ErrEnterChangedState})
	}
	m.data = data
	if next, postpone := a.actions(&m, effs, &out); postpone || len(next) > 0 {
		return m, append(out, gen.Stop{Reason: ErrEnterAction})
	}
	return m, out
}

// actions performs the actions among effs on m, turning timeouts into gen
// timers, and appends the other effects to out. It returns the events to
// insert, and whether to postpone the event.
func (a adapter[St, D]) actions(m *Machine[St, D], effs []gen.Effect, out *[]gen.Effect) (next []Event, postpone bool) {
	for _, e := range effs {
		switch x := e.(type) {
		case Postpone:
			postpone = true
		case NextEvent:
			next = append(next, x.Event)
		case StartStateTimeout:
			*out = append(*out, gen.StartTimer{Key: stateTimerKey{}, After: x.After, Msg: fired{stateTimerKey{}, x.Msg}})
			m.stateTimer = true
		case CancelStateTimeout:
			if m.stateTimer {
				*out = append(*out, gen.CancelTimer{Key: stateTimerKey{}})
				m.stateTimer = false
			}
		case StartEventTimeout:
			*out = append(*out, gen.StartTimer{Key: eventTimerKey{}, After: x.After, Msg: fired{eventTimerKey{}, x.Msg}})
			m.eventTimer = true
		case StartTimeout:
			key := genericTimerKey{x.Name}
			*out = append(*out, gen.StartTimer{Key: key, After: x.After, Msg: fired{key, x.Msg}})
		case CancelTimeout:
			*out = append(*out, gen.CancelTimer{Key: genericTimerKey{x.Name}})
		default:
			*out = append(*out, e)
		}
	}
	return next, postpone
}

func isAction(e gen.Effect) bool {
	switch e.(type) {
	case Postpone, NextEvent, StartStateTimeout, CancelStateTimeout, StartEventTimeout, StartTimeout, CancelTimeout:
		return true
	}
	return false
}

func stops(out []gen.Effect) bool {
	return slices.ContainsFunc(out, func(e gen.Effect) bool {
		_, ok := e.(gen.Stop)
		return ok
	})
}
