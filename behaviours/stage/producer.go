package stage

import (
	"fmt"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Producer is a stage with state S emitting events, with Emit, as its
// consumer demands them. Its events are of the type of its first Emit.
//
// A Producer may also implement InfoHandler and Terminator, and
// actor.Clocked, to be given the clock of its process.
type Producer[S any] interface {
	// Init returns the initial state. self is the PID of the producer.
	Init(self proc.PID) (S, []actor.Effect, error)
	// HandleDemand is told that the consumer wants n more events, beyond
	// those asked for before. The producer emits them now, or later from
	// HandleInfo, as they come: from an Async reading a file, say.
	HandleDemand(state S, n int) (S, []actor.Effect)
}

// ProducerState is the state of a producer process, as gen and gensim see
// it: the state of the behaviour, with its consumer and its buffer.
type ProducerState[S any] struct {
	state S

	consumer   subRef
	subscribed bool
	demand     int   // the events asked for and not sent
	buffer     batch // the events emitted and not asked for, while demand is 0
	emits      batch // no events, of the type of those emitted, once one is
}

// State returns the state of the behaviour.
func (p ProducerState[S]) State() S { return p.state }

// Produce makes b a stage, to start.
func Produce[S any](b Producer[S]) Stage[ProducerState[S]] {
	return producer[S]{b}
}

// producer runs a Producer as a gen.Behaviour.
type producer[S any] struct {
	b Producer[S]
}

func (producer[S]) stage() {}

// WithClock gives the clock to the behaviour, if it is actor.Clocked.
func (a producer[S]) WithClock(now func() time.Time) any {
	c, ok := a.b.(actor.Clocked)
	if !ok {
		return a
	}
	b, ok := c.WithClock(now).(Producer[S])
	if !ok {
		return nil // the runtime fails the start
	}
	return producer[S]{b}
}

func (a producer[S]) Init(self proc.PID, _ any) (ProducerState[S], []actor.Effect, error) {
	s, effs, err := a.b.Init(self)
	if err != nil {
		return ProducerState[S]{}, nil, err
	}
	p := ProducerState[S]{state: s}
	out := p.perform(effs)
	return p, out, nil
}

func (a producer[S]) Handle(p ProducerState[S], msg gen.Msg) (ProducerState[S], []actor.Effect) {
	switch m := msg.(type) {
	case gen.InfoMsg:
		return a.info(p, m.Msg)
	case gen.ContinueMsg:
		return a.handleInfo(p, m.Msg)
	}
	return a.handleInfo(p, msg)
}

// info handles a message of the stages, or else hands it to HandleInfo.
func (a producer[S]) info(p ProducerState[S], msg any) (ProducerState[S], []actor.Effect) {
	switch m := msg.(type) {
	case subscribeMsg:
		return a.subscribe(p, m)
	case askMsg:
		return a.ask(p, m)
	case cancelMsg:
		return p.cancelled(m.Ref)
	case actor.Down:
		if t, ok := m.Tag.(consumerDown); ok {
			return p.consumerGone(t.ref), nil
		}
	}
	return a.handleInfo(p, msg)
}

// subscribe takes a consumer, if the producer has none, and its demand.
func (a producer[S]) subscribe(p ProducerState[S], m subscribeMsg) (ProducerState[S], []actor.Effect) {
	if p.subscribed {
		return p, actor.Do(actor.Send{To: m.Ref.Consumer, Msg: cancelMsg{Ref: m.Ref, Reason: ErrHasConsumer}})
	}
	p.consumer, p.subscribed = m.Ref, true
	out := actor.Do(actor.Monitor{Target: m.Ref.Consumer, Tag: consumerDown{m.Ref}})
	p, more := a.ask(p, askMsg{Ref: m.Ref, N: m.Ask})
	return p, append(out, more...)
}

// ask adds to the demand: the buffer is sent first, and HandleDemand is
// told the rest.
func (a producer[S]) ask(p ProducerState[S], m askMsg) (ProducerState[S], []actor.Effect) {
	if !p.subscribed || m.Ref != p.consumer {
		return p, nil // from a consumer gone
	}
	p.demand += m.N
	sent, out := p.flush()
	n := m.N - sent
	if n <= 0 {
		return p, out
	}
	s, effs := a.b.HandleDemand(p.state, n)
	p.state = s
	return p, append(out, p.perform(effs)...)
}

// cancelled ends the subscription its consumer cancelled.
func (p ProducerState[S]) cancelled(ref subRef) (ProducerState[S], []actor.Effect) {
	if !p.subscribed || ref != p.consumer {
		return p, nil
	}
	return p.consumerGone(ref), actor.Do(actor.Demonitor{Tag: consumerDown{ref}})
}

// consumerGone forgets the consumer of ref, and its demand. The buffer
// waits for the next one.
func (p ProducerState[S]) consumerGone(ref subRef) ProducerState[S] {
	if p.subscribed && ref == p.consumer {
		p.consumer, p.subscribed, p.demand = subRef{}, false, 0
	}
	return p
}

func (a producer[S]) handleInfo(p ProducerState[S], msg any) (ProducerState[S], []actor.Effect) {
	h, ok := a.b.(InfoHandler[S])
	if !ok {
		return p, nil
	}
	s, effs := h.HandleInfo(p.state, msg)
	p.state = s
	return p, p.perform(effs)
}

// perform emits the Emits among effs, in order with the other effects,
// which it returns for the runtime.
func (p *ProducerState[S]) perform(effs []actor.Effect) []actor.Effect {
	var out []actor.Effect
	for _, e := range effs {
		switch x := e.(type) {
		case emitter:
			b := x.batch()
			if p.emits == nil {
				p.emits = b.empty()
			}
			if !p.emits.sameType(b) {
				return append(out, stopBad(e))
			}
			out = append(out, p.emit(b)...)
		case Subscribe, Cancel:
			return append(out, stopBad(e))
		default:
			out = append(out, e)
		}
	}
	return out
}

// emit sends the consumer as many events of b as it asked for, and
// buffers the rest. The buffer is empty while there is demand: events
// emitted go after it, in order.
func (p *ProducerState[S]) emit(b batch) []actor.Effect {
	n := 0
	if p.subscribed {
		n = min(p.demand, b.len())
	}
	var out []actor.Effect
	if n > 0 {
		out = actor.Do(p.send(b, n))
	}
	rest := b.tail(n)
	if rest == nil {
		return out
	}
	if p.buffer == nil {
		p.buffer = rest
	} else {
		p.buffer = p.buffer.join(rest)
	}
	return out
}

// flush sends the consumer the buffer, as far as it asked for it, and
// returns how many events it sent.
func (p *ProducerState[S]) flush() (int, []actor.Effect) {
	if p.buffer == nil {
		return 0, nil
	}
	n := min(p.demand, p.buffer.len())
	if n == 0 {
		return 0, nil
	}
	out := actor.Do(p.send(p.buffer, n))
	p.buffer = p.buffer.tail(n)
	return n, out
}

// send sends the first n events of b to the consumer, taking them from
// the demand.
func (p *ProducerState[S]) send(b batch, n int) actor.Effect {
	p.demand -= n
	return actor.Send{To: p.consumer.Consumer, Msg: eventsMsg{Ref: p.consumer, Events: b.head(n)}}
}

// Label tells what the process is: the producer and its behaviour.
func (a producer[S]) Label() string { return fmt.Sprintf("stage producer %T", a.b) }

// FormatStatus formats the report of the producer terminating with the
// FormatStatus of the behaviour, if it has one: see actor.StatusFormatter.
// The State is the ProducerState.
func (a producer[S]) FormatStatus(st actor.Status) actor.Status {
	if f, ok := a.b.(actor.StatusFormatter); ok {
		return f.FormatStatus(st)
	}
	return st
}

func (a producer[S]) Terminate(p ProducerState[S], reason error) []actor.Effect {
	t, ok := a.b.(Terminator[S])
	if !ok {
		return nil
	}
	return slices.DeleteFunc(t.Terminate(p.state, reason), isStageEffect)
}
