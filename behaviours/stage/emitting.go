package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// emitting is the side of a stage that emits, a producer or a
// producer-consumer: its consumers, the demand of each, and the events
// waiting for demand.
//
// Each event goes to one consumer, the one wanting most, as GenStage's
// DemandDispatcher sends them.
type emitting struct {
	consumers []downstream // copied by ask and emit before changing them
	demand    int          // the demand of all the consumers
	buffer    batch        // the events emitted and not asked for, while demand is 0
	emits     batch        // no events, of the type of those emitted, once one is
}

// downstream is a consumer of a stage.
type downstream struct {
	ref    subRef
	demand int // the events asked for and not sent
}

// accept takes the consumer of m, and monitors it.
func (e *emitting) accept(m subscribeMsg) []actor.Effect {
	e.consumers = append(slices.Clip(e.consumers), downstream{ref: m.Ref})
	return actor.Do(actor.Monitor{Target: m.Ref.Consumer, Tag: consumerDown{m.Ref}})
}

// ask adds the demand of m, sending the buffer first, and returns how
// much of it is left for events to come.
func (e *emitting) ask(m askMsg) (int, []actor.Effect) {
	i := e.find(m.Ref)
	if i < 0 {
		return 0, nil // from a consumer gone
	}
	e.consumers = slices.Clone(e.consumers)
	e.consumers[i].demand += m.N
	e.demand += m.N
	sent, out := e.flush(i)
	return max(m.N-sent, 0), out
}

// cancelled ends the subscription its consumer cancelled.
func (e *emitting) cancelled(ref subRef) []actor.Effect {
	if !e.gone(ref) {
		return nil
	}
	return actor.Do(actor.Demonitor{Tag: consumerDown{ref}})
}

// gone forgets the consumer of ref, and its demand, and reports whether
// it was a consumer. The events emitted for it go to the others.
func (e *emitting) gone(ref subRef) bool {
	i := e.find(ref)
	if i < 0 {
		return false
	}
	e.demand -= e.consumers[i].demand
	e.consumers = slices.Delete(slices.Clone(e.consumers), i, i+1)
	return true
}

// find returns the index of the consumer of ref, or -1.
func (e *emitting) find(ref subRef) int {
	return slices.IndexFunc(e.consumers, func(d downstream) bool { return d.ref == ref })
}

// emitBatch emits b, and reports whether it is of the type of the events
// emitted before.
func (e *emitting) emitBatch(b batch) ([]actor.Effect, bool) {
	if e.emits == nil {
		e.emits = b.empty()
	}
	if !e.emits.sameType(b) {
		return nil, false
	}
	return e.emit(b), true
}

// emit sends the events of b to the consumers as far as they asked for
// them, the one wanting most first, and buffers the rest. The buffer is
// empty while there is demand: events emitted go after it, in order.
func (e *emitting) emit(b batch) []actor.Effect {
	if b.len() == 0 {
		return nil
	}
	if e.demand > 0 {
		e.consumers = slices.Clone(e.consumers)
	}
	var out []actor.Effect
	for b != nil && e.demand > 0 {
		i := e.neediest()
		n := min(e.consumers[i].demand, b.len())
		out = append(out, e.send(i, b, n))
		b = b.tail(n)
	}
	if b == nil {
		return out
	}
	if e.buffer == nil {
		e.buffer = b
	} else {
		e.buffer = e.buffer.join(b)
	}
	return out
}

// neediest returns the index of the consumer wanting most, the first of
// those wanting as much.
func (e *emitting) neediest() int {
	best := 0
	for i, d := range e.consumers {
		if d.demand > e.consumers[best].demand {
			best = i
		}
	}
	return best
}

// flush sends the consumer i the buffer, as far as it asked for it, and
// returns how many events it sent. The others want none, with a buffer.
func (e *emitting) flush(i int) (int, []actor.Effect) {
	if e.buffer == nil {
		return 0, nil
	}
	n := min(e.consumers[i].demand, e.buffer.len())
	if n == 0 {
		return 0, nil
	}
	out := actor.Do(e.send(i, e.buffer, n))
	e.buffer = e.buffer.tail(n)
	return n, out
}

// send sends the first n events of b to the consumer i, taking them from
// its demand.
func (e *emitting) send(i int, b batch, n int) actor.Effect {
	d := &e.consumers[i]
	d.demand -= n
	e.demand -= n
	return actor.Send{To: d.ref.Consumer, Msg: eventsMsg{Ref: d.ref, Events: b.head(n)}}
}
