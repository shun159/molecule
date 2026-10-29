package stage

import (
	"fmt"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Producer is a stage with state S emitting events, with Emit, as its
// consumers demand them. Its events are of the type of its first Emit.
//
// A Producer may also implement InfoHandler and Terminator, and
// actor.Clocked, to be given the clock of its process.
type Producer[S any] interface {
	// Init returns the initial state. self is the PID of the producer.
	Init(self proc.PID) (S, []actor.Effect, error)
	// HandleDemand is told that the consumers want n more events, beyond
	// those asked for before. The producer emits them now, or later from
	// HandleInfo, as they come: from an Async reading a file, say.
	HandleDemand(state S, n int) (S, []actor.Effect)
}

// ProducerState is the state of a producer process, as gen and gensim see
// it: the state of the behaviour, with its consumers and its buffer.
type ProducerState[S any] struct {
	state S
	out   emitting
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
	to, err := newEmitting(a.b, self)
	if err != nil {
		return ProducerState[S]{}, nil, err
	}
	s, effs, err := a.b.Init(self)
	if err != nil {
		return ProducerState[S]{}, nil, err
	}
	p := ProducerState[S]{state: s, out: to}
	out := perform(nil, effs, &p.out, nil)
	return p, p.out.stopIfDone(out), nil
}

func (a producer[S]) Handle(p ProducerState[S], msg gen.Msg) (ProducerState[S], []actor.Effect) {
	p.out.handling()
	p, out := a.handleMsg(p, msg)
	if p.out.forward() {
		p, out = a.demanded(p, p.out.tell(), out)
	}
	return p, p.out.stopIfDone(out)
}

// handleMsg handles msg, but for stopping once done.
func (a producer[S]) handleMsg(p ProducerState[S], msg gen.Msg) (ProducerState[S], []actor.Effect) {
	switch m := msg.(type) {
	case gen.InfoMsg:
		return a.info(p, m.Msg)
	case gen.ContinueMsg:
		return a.handleInfo(p, m.Msg)
	}
	if from, ok := isStatsCall(msg); ok {
		return p, actor.Do(actor.Reply{To: from, Value: stats(&p.out, nil, 0)})
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
		n, out := p.out.cancelled(m.Ref, nil)
		return a.demanded(p, n, out)
	case actor.Down:
		if t, ok := m.Tag.(consumerDown); ok {
			n, out, _ := p.out.gone(t.ref, nil)
			return a.demanded(p, n, out)
		}
	}
	return a.handleInfo(p, msg)
}

// subscribe takes a consumer, and its demand.
func (a producer[S]) subscribe(p ProducerState[S], m subscribeMsg) (ProducerState[S], []actor.Effect) {
	out := p.out.accept(m, nil)
	n, out := p.out.ask(askMsg{Ref: m.Ref, N: m.Ask}, out)
	return a.demanded(p, n, out)
}

// ask adds to the demand: what waits is sent first, and HandleDemand is
// told the rest.
func (a producer[S]) ask(p ProducerState[S], m askMsg) (ProducerState[S], []actor.Effect) {
	n, out := p.out.ask(m, nil)
	return a.demanded(p, n, out)
}

// demanded tells HandleDemand of n more events wanted, if any, after the
// effects out.
func (a producer[S]) demanded(p ProducerState[S], n int, out []actor.Effect) (ProducerState[S], []actor.Effect) {
	if n == 0 {
		return p, out
	}
	s, effs := a.b.HandleDemand(p.state, n)
	p.state = s
	return p, perform(out, effs, &p.out, nil)
}

func (a producer[S]) handleInfo(p ProducerState[S], msg any) (ProducerState[S], []actor.Effect) {
	h, ok := a.b.(InfoHandler[S])
	if !ok {
		return p, nil
	}
	s, effs := h.HandleInfo(p.state, msg)
	p.state = s
	return p, perform(nil, effs, &p.out, nil)
}

// Label tells what the process is: the producer and its behaviour.
func (a producer[S]) Label() string { return fmt.Sprintf("stage producer %T", a.b) }

// FormatStatus formats the report of the producer terminating with the
// FormatStatus of the behaviour, if it has one: see actor.StatusFormatter.
// The State is the ProducerState.
func (a producer[S]) FormatStatus(st actor.Status) actor.Status { return formatStatus(a.b, st) }

func (a producer[S]) Terminate(p ProducerState[S], reason error) []actor.Effect {
	return terminate(a.b, p.state, reason)
}
