package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// demandDispatch sends each event to the consumer wanting most.
type demandDispatch struct {
	consumers []downstream // copied before changing them
	demand    int          // the demand of all
	buffer    batch        // the events not asked for, while demand is 0
}

func (d demandDispatch) subscribe(m subscribeMsg) (dispatch, error) {
	d.consumers = append(slices.Clip(d.consumers), downstream{ref: m.Ref})
	return d, nil
}

// ask sends the consumer the buffer first: the others want none, with a
// buffer.
func (d demandDispatch) ask(ref subRef, n int) (dispatch, []actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return d, nil, false
	}
	d.consumers = slices.Clone(d.consumers)
	d.consumers[i].demand += n
	d.demand += n
	out := d.flush(i)
	return d, out, true
}

func (d demandDispatch) leave(ref subRef) (dispatch, []actor.Effect, bool) {
	i := findConsumer(d.consumers, ref)
	if i < 0 {
		return d, nil, false
	}
	d.demand -= d.consumers[i].demand
	d.consumers = slices.Delete(slices.Clone(d.consumers), i, i+1)
	return d, nil, true
}

// route sends the consumer wanting most as many events as it wants, then
// the next, and so on.
func (d demandDispatch) route(b batch) (dispatch, []actor.Effect, error) {
	if d.demand > 0 {
		d.consumers = slices.Clone(d.consumers)
	}
	var out []actor.Effect
	for b != nil && d.demand > 0 {
		i := d.neediest()
		n := min(d.consumers[i].demand, b.len())
		out = append(out, d.send(i, b, n))
		b = b.tail(n)
	}
	d.buffer = keep(d.buffer, b)
	return d, out, nil
}

func (d demandDispatch) capacity() int { return d.demand }

func (d demandDispatch) held() int {
	if d.buffer == nil {
		return 0
	}
	return d.buffer.len()
}

func (d demandDispatch) trim(size int, newest bool) (dispatch, int) {
	var dropped int
	d.buffer, dropped = trimBuffer(d.buffer, size, newest)
	return d, dropped
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
func (d *demandDispatch) flush(i int) []actor.Effect {
	if d.buffer == nil {
		return nil
	}
	n := min(d.consumers[i].demand, d.buffer.len())
	if n == 0 {
		return nil
	}
	out := actor.Do(d.send(i, d.buffer, n))
	d.buffer = d.buffer.tail(n)
	return out
}

// send sends the first n events of b to the consumer i, taking them from
// its demand.
func (d *demandDispatch) send(i int, b batch, n int) actor.Effect {
	d.consumers[i].demand -= n
	d.demand -= n
	return sendEvents(d.consumers[i].ref, b, n)
}
