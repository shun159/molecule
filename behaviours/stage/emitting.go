package stage

import "github.com/shun159/molecule/actor"

// emitting is the side of a stage that emits, a producer or a
// producer-consumer: its consumer, the demand of it, and the events
// waiting for demand.
type emitting struct {
	consumer   subRef
	subscribed bool
	demand     int   // the events asked for and not sent
	buffer     batch // the events emitted and not asked for, while demand is 0
	emits      batch // no events, of the type of those emitted, once one is
}

// accept takes the consumer of m, if there is none, and monitors it;
// another it cancels. It reports whether it took it.
func (e *emitting) accept(m subscribeMsg) ([]actor.Effect, bool) {
	if e.subscribed {
		return actor.Do(actor.Send{To: m.Ref.Consumer, Msg: cancelledMsg{Ref: m.Ref, Reason: ErrHasConsumer}}), false
	}
	e.consumer, e.subscribed = m.Ref, true
	return actor.Do(actor.Monitor{Target: m.Ref.Consumer, Tag: consumerDown{m.Ref}}), true
}

// ask adds the demand of m, sending the buffer first, and returns how
// much of it is left for events to come.
func (e *emitting) ask(m askMsg) (int, []actor.Effect) {
	if !e.subscribed || m.Ref != e.consumer {
		return 0, nil // from a consumer gone
	}
	e.demand += m.N
	sent, out := e.flush()
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
// it was the consumer. The buffer waits for the next.
func (e *emitting) gone(ref subRef) bool {
	if !e.subscribed || ref != e.consumer {
		return false
	}
	e.consumer, e.subscribed, e.demand = subRef{}, false, 0
	return true
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

// emit sends the consumer as many events of b as it asked for, and
// buffers the rest. The buffer is empty while there is demand: events
// emitted go after it, in order.
func (e *emitting) emit(b batch) []actor.Effect {
	n := 0
	if e.subscribed {
		n = min(e.demand, b.len())
	}
	var out []actor.Effect
	if n > 0 {
		out = actor.Do(e.send(b, n))
	}
	rest := b.tail(n)
	if rest == nil {
		return out
	}
	if e.buffer == nil {
		e.buffer = rest
	} else {
		e.buffer = e.buffer.join(rest)
	}
	return out
}

// flush sends the consumer the buffer, as far as it asked for it, and
// returns how many events it sent.
func (e *emitting) flush() (int, []actor.Effect) {
	if e.buffer == nil {
		return 0, nil
	}
	n := min(e.demand, e.buffer.len())
	if n == 0 {
		return 0, nil
	}
	out := actor.Do(e.send(e.buffer, n))
	e.buffer = e.buffer.tail(n)
	return n, out
}

// send sends the first n events of b to the consumer, taking them from
// the demand.
func (e *emitting) send(b batch, n int) actor.Effect {
	e.demand -= n
	return actor.Send{To: e.consumer.Consumer, Msg: eventsMsg{Ref: e.consumer, Events: b.head(n)}}
}
