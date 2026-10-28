package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// broadcastDispatch sends each event to every consumer, as far as the one
// wanting least wants them.
type broadcastDispatch struct {
	consumers []downstream
	buffer    batch // the events not asked for by all, while one wants none
}

func (d *broadcastDispatch) clone() dispatch {
	c := *d
	c.consumers = slices.Clone(d.consumers)
	return &c
}

func (d *broadcastDispatch) subscribe(m subscribeMsg) error {
	d.consumers = append(d.consumers, newDownstream(m.Ref))
	return nil
}

func (d *broadcastDispatch) ask(ref subRef, n int, out []actor.Effect) ([]actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return out, false
	}
	d.consumers[i].demand += n
	return d.flush(out), true
}

// leave may let the others have the buffer, the one leaving having wanted
// least.
func (d *broadcastDispatch) leave(ref subRef, out []actor.Effect) ([]actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return out, false
	}
	d.consumers = slices.Delete(d.consumers, i, i+1)
	return d.flush(out), true
}

func (d *broadcastDispatch) route(b batch, out []actor.Effect) ([]actor.Effect, error) {
	n := min(d.capacity(), b.len())
	out = d.sendAll(b, n, out)
	d.buffer = keep(d.buffer, b.tail(n))
	return out, nil
}

// capacity is the demand of the consumer wanting least, 0 without any.
func (d *broadcastDispatch) capacity() int {
	if len(d.consumers) == 0 {
		return 0
	}
	least := d.consumers[0].demand
	for _, c := range d.consumers[1:] {
		least = min(least, c.demand)
	}
	return least
}

func (d *broadcastDispatch) consumerCount() int { return len(d.consumers) }

func (d *broadcastDispatch) held() int { return lenOf(d.buffer) }

func (d *broadcastDispatch) trim(size int, newest bool) int {
	var dropped int
	d.buffer, dropped = trimBuffer(d.buffer, size, newest)
	return dropped
}

// flush sends the buffer to all, as far as they all asked for it.
func (d *broadcastDispatch) flush(out []actor.Effect) []actor.Effect {
	if d.buffer == nil {
		return out
	}
	n := min(d.capacity(), d.buffer.len())
	out = d.sendAll(d.buffer, n, out)
	d.buffer = d.buffer.tail(n)
	return out
}

// sendAll sends the first n events of b to every consumer, taking them
// from their demand.
func (d *broadcastDispatch) sendAll(b batch, n int, out []actor.Effect) []actor.Effect {
	if n == 0 {
		return out
	}
	for i := range d.consumers {
		d.consumers[i].demand -= n
		out = append(out, sendEvents(d.consumers[i], b, n))
	}
	return out
}
