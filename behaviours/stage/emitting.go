package stage

import (
	"errors"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// emitting is the side of a stage that emits, a producer or a
// producer-consumer: the dispatch of its events to its consumers, and the
// demand it was told of. Its methods append their effects to out.
type emitting struct {
	self    proc.PID
	to      dispatch
	mine    bool // to is a clone, made in the handling of this message
	buffer  Buffer
	waiting int   // the events the stage was told of and has not emitted
	emits   batch // no events, of the type of those emitted, once one is
	done    bool  // the stage has emitted all, and stops once it is sent

	holding   bool // the demand is held, until a Forward
	forwarded bool // a Forward has just ended holding it

	emitted, dropped uint64 // events, since the start, see Stats
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
	acc, ok := b.(Accumulating)
	holding := ok && acc.Accumulate()
	return emitting{self: self, to: to, buffer: bufferOf(b), holding: holding}, err
}

// handling starts the handling of a message: the dispatch is that of the
// state given, not to be written.
func (e *emitting) handling() { e.mine = false }

// change returns the dispatch to change: a clone of that of the state
// given, the first time in the handling of a message.
func (e *emitting) change() dispatch {
	if !e.mine {
		e.to, e.mine = e.to.clone(), true
	}
	return e.to
}

// accept takes the consumer of m, and monitors it, or cancels it if the
// dispatch will not have it.
func (e *emitting) accept(m subscribeMsg, out []actor.Effect) []actor.Effect {
	if err := e.change().subscribe(m); err != nil {
		return append(out, actor.Send{To: m.Ref.Consumer, Msg: cancelledMsg{Ref: m.Ref, Reason: err}})
	}
	return append(out, actor.Monitor{Target: m.Ref.Consumer, Tag: consumerDown{m.Ref}})
}

// ask adds the demand of m, sending what waits first, and returns how
// much more the stage is to emit.
func (e *emitting) ask(m askMsg, out []actor.Effect) (int, []actor.Effect) {
	out, ok := e.change().ask(m.Ref, m.N, out)
	if !ok {
		return 0, out // from a consumer gone
	}
	return e.tell(), out
}

// cancelled ends the subscription its consumer cancelled, and returns how
// much more the stage is to emit.
func (e *emitting) cancelled(ref subRef, out []actor.Effect) (int, []actor.Effect) {
	n, out, ok := e.gone(ref, out)
	if !ok {
		return 0, out
	}
	return n, append(out, actor.Demonitor{Tag: consumerDown{ref}})
}

// gone forgets the consumer of ref, reports whether it was a consumer,
// and returns how much more the stage is to emit: the others may want
// more than it did.
func (e *emitting) gone(ref subRef, out []actor.Effect) (int, []actor.Effect, bool) {
	out, ok := e.change().leave(ref, out)
	if !ok {
		return 0, out, false
	}
	return e.tell(), out, true
}

// tell returns how much more the stage is to emit than it was told of:
// none while it holds the demand.
func (e *emitting) tell() int {
	if e.holding {
		return 0
	}
	n := max(e.to.capacity()-e.waiting, 0)
	e.waiting += n
	return n
}

// forward ends holding the demand, and reports whether it did now, for
// the stage to be told of it.
func (e *emitting) forward() bool {
	if !e.forwarded {
		return false
	}
	e.forwarded = false
	return true
}

// wants reports whether events emitted now would be sent, the demand not
// held.
func (e *emitting) wants() bool { return !e.holding && e.to.capacity() > 0 }

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
func (e *emitting) emitBatch(b batch, out []actor.Effect) ([]actor.Effect, error) {
	if e.emits == nil {
		e.emits = b.empty()
	}
	if !e.emits.sameType(b) {
		return out, errEmitType
	}
	if b.len() == 0 {
		return out, nil
	}
	e.waiting = max(e.waiting-b.len(), 0)
	e.emitted += uint64(b.len())
	out, err := e.change().route(b, out)
	if err != nil {
		return out, err
	}
	return e.bound(out), nil
}
