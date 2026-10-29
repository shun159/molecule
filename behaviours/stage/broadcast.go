package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// broadcastDispatch sends each event to every consumer, as far as the one
// wanting least wants them.
type broadcastDispatch struct {
	consumers []downstream // copied before changing them
	buffer    batch        // the events not asked for by all, while one wants none
}

func (d broadcastDispatch) subscribe(m subscribeMsg) (dispatch, error) {
	d.consumers = append(slices.Clip(d.consumers), downstream{ref: m.Ref})
	return d, nil
}

func (d broadcastDispatch) ask(ref subRef, n int) (dispatch, []actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return d, nil, false
	}
	d.consumers = slices.Clone(d.consumers)
	d.consumers[i].demand += n
	out := d.flush()
	return d, out, true
}

// leave may let the others have the buffer, the one leaving having wanted
// least.
func (d broadcastDispatch) leave(ref subRef) (dispatch, []actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return d, nil, false
	}
	d.consumers = slices.Delete(slices.Clone(d.consumers), i, i+1)
	out := d.flush()
	return d, out, true
}

func (d broadcastDispatch) route(b batch) (dispatch, []actor.Effect, error) {
	d.consumers = slices.Clone(d.consumers)
	n := min(d.capacity(), b.len())
	out := d.sendAll(b, n)
	d.buffer = keep(d.buffer, b.tail(n))
	return d, out, nil
}

// capacity is the demand of the consumer wanting least, 0 without any.
func (d broadcastDispatch) capacity() int {
	if len(d.consumers) == 0 {
		return 0
	}
	least := d.consumers[0].demand
	for _, c := range d.consumers[1:] {
		least = min(least, c.demand)
	}
	return least
}

func (d broadcastDispatch) holds() bool { return d.buffer != nil }

// flush sends the buffer to all, as far as they all asked for it. The
// consumers are copied.
func (d *broadcastDispatch) flush() []actor.Effect {
	if d.buffer == nil {
		return nil
	}
	n := min(d.capacity(), d.buffer.len())
	out := d.sendAll(d.buffer, n)
	d.buffer = d.buffer.tail(n)
	return out
}

// sendAll sends the first n events of b to every consumer, taking them
// from their demand. The consumers are copied.
func (d *broadcastDispatch) sendAll(b batch, n int) []actor.Effect {
	if n == 0 {
		return nil
	}
	out := make([]actor.Effect, len(d.consumers))
	for i := range d.consumers {
		d.consumers[i].demand -= n
		out[i] = sendEvents(d.consumers[i].ref, b, n)
	}
	return out
}
