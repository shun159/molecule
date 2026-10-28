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
// A Consumer may also implement InfoHandler, CancelHandler and
// Terminator, and actor.Clocked, to be given the clock of its process.
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
	in    subscribing
}

// State returns the state of the behaviour.
func (c ConsumerState[S]) State() S { return c.state }

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
	c := ConsumerState[S]{state: s, in: subscribing{self: self}}
	out := perform(nil, effs, nil, &c.in)
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
	case cancelledMsg:
		return a.ended(c, m.Ref, m.Reason, actor.Demonitor{Tag: producerDown{m.Ref}})
	case actor.Down:
		if t, ok := m.Tag.(producerDown); ok {
			return a.ended(c, t.ref, m.Reason)
		}
	}
	return a.handleInfo(c, msg)
}

// events hands the events of m to HandleEvents, Max-Min at most at a
// time, asking for more after each batch as needed.
func (a consumer[S, In]) events(c ConsumerState[S], m eventsMsg) (ConsumerState[S], []actor.Effect) {
	if c.in.find(m.Ref) < 0 {
		return c, nil // of a subscription cancelled
	}
	events, ok := eventsOf[In](m)
	if !ok {
		return c, actor.Do(actor.Stop{Reason: &BadEventsError{Events: m.Events}})
	}
	c.in.subs = slices.Clone(c.in.subs) // the pending demands change
	var out []actor.Effect
	for len(events) > 0 {
		i := c.in.find(m.Ref)
		if i < 0 {
			break // cancelled by HandleEvents
		}
		sub := &c.in.subs[i]
		n := min(len(events), sub.batchSize())
		batch := events[:n:n]
		events = events[n:]
		ask := sub.take(n)

		s, effs := a.b.HandleEvents(c.state, batch, sub.tag)
		c.state = s
		out = perform(out, effs, nil, &c.in)
		if stops(out) {
			break
		}
		out = c.in.askMore(m.Ref, ask, out)
	}
	return c, out
}

// ended ends the subscription of ref its producer ended, exiting or
// cancelling it: HandleCancel is told, and the consumer stops with the
// same reason, or goes on, as the Ending of the subscription says.
func (a consumer[S, In]) ended(c ConsumerState[S], ref subRef, reason error, effs ...actor.Effect) (ConsumerState[S], []actor.Effect) {
	sub, ok := c.in.end(ref)
	if !ok {
		return c, nil
	}
	out := effs
	if h, ok := a.b.(CancelHandler[S]); ok {
		s, more := h.HandleCancel(c.state, sub.tag, reason)
		c.state = s
		out = perform(out, more, nil, &c.in)
	}
	if sub.ending.stops(reason) && !stops(out) {
		out = append(out, actor.Stop{Reason: reason})
	}
	return c, out
}

func (a consumer[S, In]) handleInfo(c ConsumerState[S], msg any) (ConsumerState[S], []actor.Effect) {
	h, ok := a.b.(InfoHandler[S])
	if !ok {
		return c, nil
	}
	s, effs := h.HandleInfo(c.state, msg)
	c.state = s
	return c, perform(nil, effs, nil, &c.in)
}

// Label tells what the process is: the consumer and its behaviour.
func (a consumer[S, In]) Label() string { return fmt.Sprintf("stage consumer %T", a.b) }

// FormatStatus formats the report of the consumer terminating with the
// FormatStatus of the behaviour, if it has one: see actor.StatusFormatter.
// The State is the ConsumerState.
func (a consumer[S, In]) FormatStatus(st actor.Status) actor.Status { return formatStatus(a.b, st) }

func (a consumer[S, In]) Terminate(c ConsumerState[S], reason error) []actor.Effect {
	return terminate(a.b, c.state, reason)
}
