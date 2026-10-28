package stage

import (
	"fmt"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// ProducerConsumer is a stage with state S handling events of type In
// from the producers it subscribes to, as a Consumer does, and emitting
// events to its own consumers, as a Producer does: a step of a pipeline,
// mapping, filtering or splitting events.
//
// It handles events only as its consumers demand them: the events of its
// producers wait in it meanwhile, and it asks them for more only once it
// has handled those. A filter, emitting less than it is given, handles
// more until its consumers have what they asked for.
//
// A subscription of it ending normally, its producer done, and Permanent,
// it stops once it has handled the events of that producer and sent its
// consumers those it emitted, so that a pipeline ends once all is
// through; another reason stops it at once. A subscription it goes on
// without, its events received are handled all the same.
//
// A ProducerConsumer may also implement InfoHandler, CancelHandler and
// Terminator, and actor.Clocked, to be given the clock of its process.
type ProducerConsumer[S, In any] interface {
	// Init returns the initial state. self is the PID of the stage.
	Init(self proc.PID) (S, []actor.Effect, error)
	// HandleEvents handles events of the subscription under tag, in the
	// order the producer emitted them, and emits events with Emit. The
	// events are not to be written.
	HandleEvents(state S, events []In, tag any) (S, []actor.Effect)
}

// ProducerConsumerState is the state of a producer-consumer process, as
// gen and gensim see it: the state of the behaviour, with its
// subscriptions, its consumers, and the events waiting either way.
type ProducerConsumerState[S, In any] struct {
	state S
	in    subscribing
	out   emitting

	queue []queued[In] // the events received, to handle as demand comes

	// A subscription ended normally: the stage handles its events left and
	// stops.
	ending bool
	endRef subRef
	reason error
}

// State returns the state of the behaviour.
func (p ProducerConsumerState[S, In]) State() S { return p.state }

// queued is a batch of events received, with how many more to ask for
// once handled.
type queued[In any] struct {
	ref    subRef
	tag    any
	events []In
	ask    int
}

// ProduceConsume makes b a stage, to start.
func ProduceConsume[S, In any](b ProducerConsumer[S, In]) Stage[ProducerConsumerState[S, In]] {
	return producerConsumer[S, In]{b}
}

// producerConsumer runs a ProducerConsumer as a gen.Behaviour.
type producerConsumer[S, In any] struct {
	b ProducerConsumer[S, In]
}

func (producerConsumer[S, In]) stage() {}

// WithClock gives the clock to the behaviour, if it is actor.Clocked.
func (a producerConsumer[S, In]) WithClock(now func() time.Time) any {
	c, ok := a.b.(actor.Clocked)
	if !ok {
		return a
	}
	b, ok := c.WithClock(now).(ProducerConsumer[S, In])
	if !ok {
		return nil // the runtime fails the start
	}
	return producerConsumer[S, In]{b}
}

func (a producerConsumer[S, In]) Init(self proc.PID, _ any) (ProducerConsumerState[S, In], []actor.Effect, error) {
	to, err := newEmitting(a.b, self)
	if err != nil {
		return ProducerConsumerState[S, In]{}, nil, err
	}
	s, effs, err := a.b.Init(self)
	if err != nil {
		return ProducerConsumerState[S, In]{}, nil, err
	}
	p := ProducerConsumerState[S, In]{state: s, in: subscribing{self: self}, out: to}
	out := perform(effs, &p.out, &p.in)
	return p, p.out.stopIfDone(out), nil
}

func (a producerConsumer[S, In]) Handle(p ProducerConsumerState[S, In], msg gen.Msg) (ProducerConsumerState[S, In], []actor.Effect) {
	p, out := a.handleMsg(p, msg)
	return p, p.out.stopIfDone(out)
}

// handleMsg handles msg, but for stopping once done.
func (a producerConsumer[S, In]) handleMsg(p ProducerConsumerState[S, In], msg gen.Msg) (ProducerConsumerState[S, In], []actor.Effect) {
	switch m := msg.(type) {
	case gen.InfoMsg:
		return a.info(p, m.Msg)
	case gen.ContinueMsg:
		return a.handleInfo(p, m.Msg)
	}
	return a.handleInfo(p, msg)
}

// info handles a message of the stages, from its consumers or its
// producers, or else hands it to HandleInfo.
func (a producerConsumer[S, In]) info(p ProducerConsumerState[S, In], msg any) (ProducerConsumerState[S, In], []actor.Effect) {
	switch m := msg.(type) {
	case subscribeMsg:
		return a.subscribe(p, m)
	case askMsg:
		return a.ask(p, m)
	case cancelMsg:
		_, out := p.out.cancelled(m.Ref)
		return a.take(p, out)
	case eventsMsg:
		return a.events(p, m)
	case cancelledMsg:
		return a.ended(p, m.Ref, m.Reason, actor.Demonitor{Tag: producerDown{m.Ref}})
	case actor.Down:
		return a.down(p, m)
	}
	return a.handleInfo(p, msg)
}

// down handles the end of a consumer or of a producer.
func (a producerConsumer[S, In]) down(p ProducerConsumerState[S, In], d actor.Down) (ProducerConsumerState[S, In], []actor.Effect) {
	switch t := d.Tag.(type) {
	case consumerDown:
		_, out, _ := p.out.gone(t.ref)
		return a.take(p, out)
	case producerDown:
		return a.ended(p, t.ref, d.Reason)
	}
	return a.handleInfo(p, d)
}

// subscribe takes a consumer, and its demand.
func (a producerConsumer[S, In]) subscribe(p ProducerConsumerState[S, In], m subscribeMsg) (ProducerConsumerState[S, In], []actor.Effect) {
	out := p.out.accept(m)
	p, more := a.ask(p, askMsg{Ref: m.Ref, N: m.Ask})
	return p, append(out, more...)
}

// ask adds to the demand of a consumer: the buffer is sent first, and
// the events queued are handled for the rest.
func (a producerConsumer[S, In]) ask(p ProducerConsumerState[S, In], m askMsg) (ProducerConsumerState[S, In], []actor.Effect) {
	_, out := p.out.ask(m)
	return a.take(p, out)
}

// events queues the events of m, in batches of Max-Min at most, and
// handles them as far as there is demand.
func (a producerConsumer[S, In]) events(p ProducerConsumerState[S, In], m eventsMsg) (ProducerConsumerState[S, In], []actor.Effect) {
	i := p.in.find(m.Ref)
	if i < 0 {
		return p, nil // of a subscription cancelled
	}
	events, ok := m.Events.([]In)
	if !ok {
		return p, actor.Do(actor.Stop{Reason: &BadEventsError{Events: m.Events}})
	}
	p.in.subs = slices.Clone(p.in.subs) // the pending demand changes
	sub := &p.in.subs[i]
	p.queue = slices.Clip(p.queue)
	for len(events) > 0 {
		n := min(len(events), sub.batchSize())
		p.queue = append(p.queue, queued[In]{ref: m.Ref, tag: sub.tag, events: events[:n:n], ask: sub.take(n)})
		events = events[n:]
	}
	return a.take(p, nil)
}

// take handles the batches queued while the consumers want events, asking
// their producers for more after each, and stops the stage once it has
// ended. A batch of a subscription ended is handled all the same, unless
// the stage is ending.
func (a producerConsumer[S, In]) take(p ProducerConsumerState[S, In], out []actor.Effect) (ProducerConsumerState[S, In], []actor.Effect) {
	for p.out.wants() && len(p.queue) > 0 {
		q := p.queue[0]
		p.queue = p.queue[1:]
		if !p.live(q.ref) {
			continue
		}
		if out = a.handle(&p, q, out); stops(out) {
			return p, out
		}
	}
	if len(p.queue) == 0 {
		p.queue = nil
	}
	return p, p.stopIfDrained(out)
}

// handle hands the batch q to HandleEvents, and then asks its producer
// for more, as q says.
func (a producerConsumer[S, In]) handle(p *ProducerConsumerState[S, In], q queued[In], out []actor.Effect) []actor.Effect {
	s, effs := a.b.HandleEvents(p.state, q.events, q.tag)
	p.state = s
	out = append(out, perform(effs, &p.out, &p.in)...)
	if stops(out) {
		return out
	}
	return p.in.askMore(q.ref, q.ask, out)
}

// live reports whether the events of ref are to be handled: all, of
// subscriptions on or ended, but once the stage is ending, those of the
// one ended only.
func (p ProducerConsumerState[S, In]) live(ref subRef) bool {
	return !p.ending || ref == p.endRef
}

// ended ends the subscription of ref its producer ended, exiting or
// cancelling it: HandleCancel is told, and as the Ending of the
// subscription says, the stage goes on, handling the events it holds of
// it, or stops: with a normal reason, once it has handled them and sent
// what it emitted, with another, now.
func (a producerConsumer[S, In]) ended(p ProducerConsumerState[S, In], ref subRef, reason error, effs ...actor.Effect) (ProducerConsumerState[S, In], []actor.Effect) {
	sub, ok := p.in.end(ref)
	if !ok {
		return p, nil
	}
	out := effs
	if h, ok := a.b.(CancelHandler[S]); ok {
		s, more := h.HandleCancel(p.state, sub.tag, reason)
		p.state = s
		out = append(out, perform(more, &p.out, &p.in)...)
	}
	switch {
	case stops(out) || !sub.ending.stops(reason):
		return p, out
	case !normal(reason):
		return p, append(out, actor.Stop{Reason: reason})
	}
	if !p.ending {
		p.ending, p.endRef, p.reason = true, ref, reason
	}
	return p, p.stopIfDrained(out)
}

// stopIfDrained stops the stage ending once it has handled the events of
// the subscription ended and sent its buffer.
func (p ProducerConsumerState[S, In]) stopIfDrained(out []actor.Effect) []actor.Effect {
	if !p.ending || p.out.holds() {
		return out
	}
	if slices.ContainsFunc(p.queue, func(q queued[In]) bool { return q.ref == p.endRef }) {
		return out
	}
	return append(out, actor.Stop{Reason: p.reason})
}

func (a producerConsumer[S, In]) handleInfo(p ProducerConsumerState[S, In], msg any) (ProducerConsumerState[S, In], []actor.Effect) {
	h, ok := a.b.(InfoHandler[S])
	if !ok {
		return p, nil
	}
	s, effs := h.HandleInfo(p.state, msg)
	p.state = s
	return p, perform(effs, &p.out, &p.in)
}

// Label tells what the process is: the producer-consumer and its
// behaviour.
func (a producerConsumer[S, In]) Label() string {
	return fmt.Sprintf("stage producer-consumer %T", a.b)
}

// FormatStatus formats the report of the stage terminating with the
// FormatStatus of the behaviour, if it has one: see actor.StatusFormatter.
// The State is the ProducerConsumerState.
func (a producerConsumer[S, In]) FormatStatus(st actor.Status) actor.Status {
	return formatStatus(a.b, st)
}

func (a producerConsumer[S, In]) Terminate(p ProducerConsumerState[S, In], reason error) []actor.Effect {
	return terminate(a.b, p.state, reason)
}
