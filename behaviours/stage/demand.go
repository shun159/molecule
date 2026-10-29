package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// demandDispatch sends each event to the consumer wanting most.
type demandDispatch struct {
	consumers []downstream
	demand    int   // the demand of all
	buffer    batch // the events not asked for, while demand is 0
}

func (d *demandDispatch) clone() dispatch {
	c := *d
	c.consumers = slices.Clone(d.consumers)
	return &c
}

func (d *demandDispatch) subscribe(m subscribeMsg) error {
	d.consumers = append(d.consumers, newDownstream(m.Ref))
	return nil
}

// ask sends the consumer the buffer first: the others want none, with a
// buffer.
func (d *demandDispatch) ask(ref subRef, n int, out []actor.Effect) ([]actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return out, false
	}
	d.consumers[i].demand += n
	d.demand += n
	return d.flush(i, out), true
}

func (d *demandDispatch) leave(ref subRef, out []actor.Effect) ([]actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return out, false
	}
	d.demand -= d.consumers[i].demand
	d.consumers = slices.Delete(d.consumers, i, i+1)
	return out, true
}

// route sends the consumer wanting most as many events as it wants, then
// the next, and so on.
func (d *demandDispatch) route(b batch, out []actor.Effect) ([]actor.Effect, error) {
	for b != nil && d.demand > 0 {
		i := d.neediest()
		n := min(d.consumers[i].demand, b.len())
		out = append(out, d.send(i, b, n))
		b = b.tail(n)
	}
	d.buffer = keep(d.buffer, b)
	return out, nil
}

func (d *demandDispatch) capacity() int { return d.demand }

func (d *demandDispatch) consumerCount() int { return len(d.consumers) }

func (d *demandDispatch) dropped() uint64 { return 0 }

func (d *demandDispatch) held() int { return lenOf(d.buffer) }

func (d *demandDispatch) trim(size int, newest bool) int {
	var dropped int
	d.buffer, dropped = trimBuffer(d.buffer, size, newest)
	return dropped
}

// neediest returns the index of the consumer wanting most, the first of
// those wanting as much.
func (d *demandDispatch) neediest() int {
	best := 0
	for i, c := range d.consumers {
		if c.demand > d.consumers[best].demand {
			best = i
		}
	}
	return best
}

// flush sends the consumer i the buffer, as far as it asked for it.
func (d *demandDispatch) flush(i int, out []actor.Effect) []actor.Effect {
	if d.buffer == nil {
		return out
	}
	n := min(d.consumers[i].demand, d.buffer.len())
	if n == 0 {
		return out
	}
	out = append(out, d.send(i, d.buffer, n))
	d.buffer = d.buffer.tail(n)
	return out
}

// send sends the first n events of b to the consumer i, taking them from
// its demand.
func (d *demandDispatch) send(i int, b batch, n int) actor.Effect {
	d.consumers[i].demand -= n
	d.demand -= n
	return sendEvents(d.consumers[i], b, n)
}
