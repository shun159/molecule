package stage

import (
	"errors"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// emitting is the side of a stage that emits, a producer or a
// producer-consumer: the dispatch of its events to its consumers, and the
// demand it was told of.
type emitting struct {
	self    proc.PID
	to      dispatch
	buffer  Buffer
	waiting int   // the events the stage was told of and has not emitted
	emits   batch // no events, of the type of those emitted, once one is
	done    bool  // the stage has emitted all, and stops once it is sent
}

// errEmitType is an Emit of another type than the events emitted before,
// or than the Hash of a Partition takes.
var errEmitType = errors.New("stage: an Emit of another type")

// newEmitting returns the emitting side of the stage b, self, with the
// Dispatcher and the Buffer it tells, if any.
func newEmitting(b any, self proc.PID) (emitting, error) {
	var d Dispatcher = Demand{}
	if x, ok := b.(Dispatching); ok && x.Dispatcher() != nil {
		d = x.Dispatcher()
	}
	to, err := d.newDispatch()
	return emitting{self: self, to: to, buffer: bufferOf(b)}, err
}

// accept takes the consumer of m, and monitors it, or cancels it if the
// dispatch will not have it.
func (e *emitting) accept(m subscribeMsg) []actor.Effect {
	to, err := e.to.subscribe(m)
	if err != nil {
		return actor.Do(actor.Send{To: m.Ref.Consumer, Msg: cancelledMsg{Ref: m.Ref, Reason: err}})
	}
	e.to = to
	return actor.Do(actor.Monitor{Target: m.Ref.Consumer, Tag: consumerDown{m.Ref}})
}

// ask adds the demand of m, sending what waits first, and returns how
// much more the stage is to emit.
func (e *emitting) ask(m askMsg) (int, []actor.Effect) {
	to, out, ok := e.to.ask(m.Ref, m.N)
	if !ok {
		return 0, nil // from a consumer gone
	}
	e.to = to
	return e.tell(), out
}

// cancelled ends the subscription its consumer cancelled, and returns how
// much more the stage is to emit.
func (e *emitting) cancelled(ref subRef) (int, []actor.Effect) {
	n, out, ok := e.gone(ref)
	if !ok {
		return 0, nil
	}
	return n, append(out, actor.Demonitor{Tag: consumerDown{ref}})
}

// gone forgets the consumer of ref, reports whether it was a consumer,
// and returns how much more the stage is to emit: the others may want
// more than it did.
func (e *emitting) gone(ref subRef) (int, []actor.Effect, bool) {
	to, out, ok := e.to.leave(ref)
	if !ok {
		return 0, nil, false
	}
	e.to = to
	return e.tell(), out, true
}

// tell returns how much more the stage is to emit than it was told of.
func (e *emitting) tell() int {
	n := max(e.to.capacity()-e.waiting, 0)
	e.waiting += n
	return n
}

// wants reports whether events emitted now would be sent.
func (e *emitting) wants() bool { return e.to.capacity() > 0 }

// holds reports whether events wait for demand.
func (e *emitting) holds() bool { return e.to.held() > 0 }

// stopIfDone stops the stage after out, if it is done and has sent all.
func (e *emitting) stopIfDone(out []actor.Effect) []actor.Effect {
	if !e.done || e.holds() || stops(out) {
		return out
	}
	return append(out, actor.Stop{})
}

// emitBatch emits b. Its error is errEmitType for b of another type than
// the events emitted before.
func (e *emitting) emitBatch(b batch) ([]actor.Effect, error) {
	if e.emits == nil {
		e.emits = b.empty()
	}
	if !e.emits.sameType(b) {
		return nil, errEmitType
	}
	if b.len() == 0 {
		return nil, nil
	}
	e.waiting = max(e.waiting-b.len(), 0)
	to, out, err := e.to.route(b)
	if err != nil {
		return nil, err
	}
	e.to = to
	return append(out, e.bound()...), nil
}
