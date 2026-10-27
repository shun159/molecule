package stage

import (
	"fmt"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Consumer is a stage with state S handling events of type In from the
// producers it subscribes to, with Subscribe: in Init, or later.
//
// A Consumer may also implement InfoHandler and Terminator, and
// actor.Clocked, to be given the clock of its process.
type Consumer[S, In any] interface {
	// Init returns the initial state. self is the PID of the consumer.
	Init(self proc.PID) (S, []actor.Effect, error)
	// HandleEvents handles events of the subscription under tag, in the
	// order the producer emitted them. They are not to be written.
	HandleEvents(state S, events []In, tag any) (S, []actor.Effect)
}

// ConsumerState is the state of a consumer process, as gen and gensim see
// it: the state of the behaviour, with its subscriptions.
type ConsumerState[S any] struct {
	state S

	self proc.PID
	subs []subscription // copied when changed
	next uint64         // the N of the next subRef
}

// State returns the state of the behaviour.
func (c ConsumerState[S]) State() S { return c.state }

// subscription is a subscription of a consumer to a producer.
type subscription struct {
	ref      subRef
	to       actor.Dest
	tag      any
	max, min int
	pending  int // the events asked for and not received
}

// take counts n events received, and returns how many to ask for: once
// the pending demand falls to min, as many as bring it back to max.
func (s *subscription) take(n int) int {
	s.pending = max(s.pending-n, 0)
	if s.pending > s.min {
		return 0
	}
	ask := s.max - s.pending
	s.pending = s.max
	return ask
}

// Consume makes b a stage, to start.
func Consume[S, In any](b Consumer[S, In]) Stage[ConsumerState[S]] {
	return consumer[S, In]{b}
}

// consumer runs a Consumer as a gen.Behaviour.
type consumer[S, In any] struct {
	b Consumer[S, In]
}

func (consumer[S, In]) stage() {}

// WithClock gives the clock to the behaviour, if it is actor.Clocked.
func (a consumer[S, In]) WithClock(now func() time.Time) any {
	c, ok := a.b.(actor.Clocked)
	if !ok {
		return a
	}
	b, ok := c.WithClock(now).(Consumer[S, In])
	if !ok {
		return nil // the runtime fails the start
	}
	return consumer[S, In]{b}
}

func (a consumer[S, In]) Init(self proc.PID, _ any) (ConsumerState[S], []actor.Effect, error) {
	s, effs, err := a.b.Init(self)
	if err != nil {
		return ConsumerState[S]{}, nil, err
	}
	c := ConsumerState[S]{state: s, self: self}
	out := c.perform(effs)
	return c, out, nil
}

func (a consumer[S, In]) Handle(c ConsumerState[S], msg gen.Msg) (ConsumerState[S], []actor.Effect) {
	switch m := msg.(type) {
	case gen.InfoMsg:
		return a.info(c, m.Msg)
	case gen.ContinueMsg:
		return a.handleInfo(c, m.Msg)
	}
	return a.handleInfo(c, msg)
}

// info handles a message of the stages, or else hands it to HandleInfo.
func (a consumer[S, In]) info(c ConsumerState[S], msg any) (ConsumerState[S], []actor.Effect) {
	switch m := msg.(type) {
	case eventsMsg:
		return a.events(c, m)
	case cancelMsg:
		return c.ended(m.Ref, m.Reason, actor.Demonitor{Tag: producerDown{m.Ref}})
	case actor.Down:
		if t, ok := m.Tag.(producerDown); ok {
			return c.ended(t.ref, m.Reason)
		}
	}
	return a.handleInfo(c, msg)
}

// events hands the events of m to HandleEvents, Max-Min at most at a
// time, asking for more after each batch as needed.
//
// It is kept whole: every event of every consumer goes through it.
//
//abc:disable abc,cyclo the loop over the events of every message
func (a consumer[S, In]) events(c ConsumerState[S], m eventsMsg) (ConsumerState[S], []actor.Effect) {
	if c.find(m.Ref) < 0 {
		return c, nil // of a subscription cancelled
	}
	events, ok := m.Events.([]In)
	if !ok {
		return c, actor.Do(actor.Stop{Reason: &BadEventsError{Events: m.Events}})
	}
	c.subs = slices.Clone(c.subs) // the pending demands change
	var out []actor.Effect
	for len(events) > 0 {
		i := c.find(m.Ref)
		if i < 0 {
			break // cancelled by HandleEvents
		}
		sub := &c.subs[i]
		n := min(len(events), sub.max-sub.min)
		batch := events[:n:n]
		events = events[n:]
		ask := sub.take(n)
		to, tag := sub.to, sub.tag

		s, effs := a.b.HandleEvents(c.state, batch, tag)
		c.state = s
		out = append(out, c.perform(effs)...)
		if stops(out) {
			break
		}
		if ask > 0 && c.find(m.Ref) >= 0 {
			out = append(out, actor.Send{To: to, Msg: askMsg{Ref: m.Ref, N: ask}})
		}
	}
	return c, out
}

// ended ends the subscription of ref its producer ended, exiting or
// cancelling it, and stops the consumer with the same reason.
func (c ConsumerState[S]) ended(ref subRef, reason error, effs ...actor.Effect) (ConsumerState[S], []actor.Effect) {
	i := c.find(ref)
	if i < 0 {
		return c, nil
	}
	c.subs = slices.Delete(slices.Clone(c.subs), i, i+1)
	return c, append(effs, actor.Stop{Reason: reason})
}

func (a consumer[S, In]) handleInfo(c ConsumerState[S], msg any) (ConsumerState[S], []actor.Effect) {
	h, ok := a.b.(InfoHandler[S])
	if !ok {
		return c, nil
	}
	s, effs := h.HandleInfo(c.state, msg)
	c.state = s
	return c, c.perform(effs)
}

// perform subscribes and cancels as effs say, in order with the other
// effects, which it returns for the runtime.
func (c *ConsumerState[S]) perform(effs []actor.Effect) []actor.Effect {
	var out []actor.Effect
	for _, e := range effs {
		switch x := e.(type) {
		case Subscribe:
			out = append(out, c.subscribe(x)...)
		case Cancel:
			out = append(out, c.cancel(x)...)
		case emitter:
			return append(out, stopBad(e))
		default:
			out = append(out, e)
		}
	}
	return out
}

// subscribe subscribes to the producer at x.To, which it monitors, asking
// for Max events.
func (c *ConsumerState[S]) subscribe(x Subscribe) []actor.Effect {
	maxDemand, minDemand, ok := x.demand()
	if !ok {
		return actor.Do(actor.Stop{Reason: ErrBadDemand})
	}
	out := c.cancel(Cancel{Tag: x.Tag})
	ref := subRef{Consumer: c.self, N: c.next}
	c.next++
	sub := subscription{ref: ref, to: x.To, tag: x.Tag, max: maxDemand, min: minDemand, pending: maxDemand}
	c.subs = append(slices.Clip(c.subs), sub)
	return append(out,
		actor.Monitor{Target: x.To, Tag: producerDown{ref}},
		actor.Send{To: x.To, Msg: subscribeMsg{Ref: ref, Ask: maxDemand}},
	)
}

// cancel cancels the subscription under x.Tag, if any.
func (c *ConsumerState[S]) cancel(x Cancel) []actor.Effect {
	i := slices.IndexFunc(c.subs, func(s subscription) bool { return s.tag == x.Tag })
	if i < 0 {
		return nil
	}
	sub := c.subs[i]
	c.subs = slices.Delete(slices.Clone(c.subs), i, i+1)
	return actor.Do(
		actor.Demonitor{Tag: producerDown{sub.ref}},
		actor.Send{To: sub.to, Msg: cancelMsg{Ref: sub.ref, Reason: x.Reason}},
	)
}

// find returns the index of the subscription of ref, or -1.
func (c ConsumerState[S]) find(ref subRef) int {
	return slices.IndexFunc(c.subs, func(s subscription) bool { return s.ref == ref })
}

// Label tells what the process is: the consumer and its behaviour.
func (a consumer[S, In]) Label() string { return fmt.Sprintf("stage consumer %T", a.b) }

// FormatStatus formats the report of the consumer terminating with the
// FormatStatus of the behaviour, if it has one: see actor.StatusFormatter.
// The State is the ConsumerState.
func (a consumer[S, In]) FormatStatus(st actor.Status) actor.Status {
	if f, ok := a.b.(actor.StatusFormatter); ok {
		return f.FormatStatus(st)
	}
	return st
}

func (a consumer[S, In]) Terminate(c ConsumerState[S], reason error) []actor.Effect {
	t, ok := a.b.(Terminator[S])
	if !ok {
		return nil
	}
	return slices.DeleteFunc(t.Terminate(c.state, reason), isStageEffect)
}
