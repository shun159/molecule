package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// partitionDispatch sends each event to the consumer of its partition.
type partitionDispatch struct {
	hash   any         // the Hash of Partition, a func(T) int
	parts  []partition // by number, copied before changing them
	queued int         // the events in the queues of all
}

// partition is a partition, and its consumer if it has one.
type partition struct {
	consumer downstream
	taken    bool
	queue    batch // the events of the partition not asked for, while its demand is 0
}

func (d partitionDispatch) subscribe(m subscribeMsg) (dispatch, error) {
	k := m.Partition
	if k < 0 || k >= len(d.parts) {
		return d, ErrBadPartition
	}
	if d.parts[k].taken {
		return d, ErrPartitionTaken
	}
	d.parts = slices.Clone(d.parts)
	d.parts[k].consumer, d.parts[k].taken = downstream{ref: m.Ref}, true
	return d, nil
}

// ask sends the consumer the queue of its partition first.
func (d partitionDispatch) ask(ref subRef, n int) (dispatch, []actor.Effect, bool) {
	k := d.find(ref)
	if k < 0 {
		return d, nil, false
	}
	d.parts = slices.Clone(d.parts)
	d.parts[k].consumer.demand += n
	out := d.flush(k)
	return d, out, true
}

// leave keeps the queue of the partition for its next consumer.
func (d partitionDispatch) leave(ref subRef) (dispatch, []actor.Effect, bool) {
	k := d.find(ref)
	if k < 0 {
		return d, nil, false
	}
	d.parts = slices.Clone(d.parts)
	d.parts[k].consumer, d.parts[k].taken = downstream{}, false
	return d, nil, true
}

func (d partitionDispatch) route(b batch) (dispatch, []actor.Effect, error) {
	split, err := b.partition(d.hash, len(d.parts))
	if err != nil {
		return d, nil, err
	}
	d.parts = slices.Clone(d.parts)
	var out []actor.Effect
	for k, pb := range split {
		if pb == nil {
			continue
		}
		p := &d.parts[k]
		n := min(p.consumer.demand, pb.len())
		if n > 0 {
			out = append(out, d.send(k, pb, n))
		}
		if rest := pb.tail(n); rest != nil {
			p.queue = keep(p.queue, rest)
			d.queued += rest.len()
		}
	}
	return d, out, nil
}

// capacity is the demand of all, less the events queued: those kept for
// some partitions count against the demand of the others.
func (d partitionDispatch) capacity() int {
	demand := 0
	for _, p := range d.parts {
		demand += p.consumer.demand
	}
	return max(demand-d.queued, 0)
}

func (d partitionDispatch) held() int { return d.queued }

// trim cuts every queue to a level, the same for all, so that no more
// than size are queued: the longest lose most.
func (d partitionDispatch) trim(size int, newest bool) (dispatch, int) {
	if d.queued <= size {
		return d, 0
	}
	level := d.level(size)
	d.parts = slices.Clone(d.parts)
	dropped := 0
	for k := range d.parts {
		var cut int
		d.parts[k].queue, cut = trimBuffer(d.parts[k].queue, level, newest)
		dropped += cut
	}
	d.queued -= dropped
	return d, dropped
}

// level returns the most events each queue may keep for no more than
// size to be queued in all.
func (d *partitionDispatch) level(size int) int {
	lo, hi := 0, size
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if d.queuedUpTo(mid) <= size {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo
}

// queuedUpTo is how many events are queued with no queue beyond level.
func (d *partitionDispatch) queuedUpTo(level int) int {
	n := 0
	for _, p := range d.parts {
		if p.queue != nil {
			n += min(p.queue.len(), level)
		}
	}
	return n
}

// find returns the partition of the consumer of ref, or -1.
func (d *partitionDispatch) find(ref subRef) int {
	return slices.IndexFunc(d.parts, func(p partition) bool { return p.taken && p.consumer.ref == ref })
}

// flush sends the consumer of partition k its queue, as far as it asked
// for it.
func (d *partitionDispatch) flush(k int) []actor.Effect {
	p := &d.parts[k]
	if p.queue == nil {
		return nil
	}
	n := min(p.consumer.demand, p.queue.len())
	if n == 0 {
		return nil
	}
	out := actor.Do(d.send(k, p.queue, n))
	p.queue = p.queue.tail(n)
	d.queued -= n
	return out
}

// send sends the first n events of b to the consumer of partition k,
// taking them from its demand.
func (d *partitionDispatch) send(k int, b batch, n int) actor.Effect {
	c := &d.parts[k].consumer
	c.demand -= n
	return sendEvents(c.ref, b, n)
}
