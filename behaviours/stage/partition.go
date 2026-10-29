package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
)

// partitionDispatch sends each event to the consumer of its partition.
type partitionDispatch struct {
	hash   any         // the Hash of Partition, a func(T) int
	parts  []partition // by number
	queued int         // the events in the queues of all
}

// partition is a partition, and its consumer if it has one.
type partition struct {
	consumer downstream
	taken    bool
	queue    batch // the events of the partition not asked for, while its demand is 0
}

func (d *partitionDispatch) clone() dispatch {
	c := *d
	c.parts = slices.Clone(d.parts)
	return &c
}

func (d *partitionDispatch) subscribe(m subscribeMsg) error {
	k := m.Partition
	if k < 0 || k >= len(d.parts) {
		return ErrBadPartition
	}
	if d.parts[k].taken {
		return ErrPartitionTaken
	}
	d.parts[k].consumer, d.parts[k].taken = newDownstream(m.Ref), true
	return nil
}

// ask sends the consumer the queue of its partition first.
func (d *partitionDispatch) ask(ref subRef, n int, out []actor.Effect) ([]actor.Effect, bool) {
	k := d.find(ref)
	if k < 0 {
		return out, false
	}
	d.parts[k].consumer.demand += n
	return d.flush(k, out), true
}

// leave keeps the queue of the partition for its next consumer.
func (d *partitionDispatch) leave(ref subRef, out []actor.Effect) ([]actor.Effect, bool) {
	k := d.find(ref)
	if k < 0 {
		return out, false
	}
	d.parts[k].consumer, d.parts[k].taken = downstream{}, false
	return out, true
}

func (d *partitionDispatch) route(b batch, out []actor.Effect) ([]actor.Effect, error) {
	split, err := b.partition(d.hash, len(d.parts))
	if err != nil {
		return out, err
	}
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
	return out, nil
}

// capacity is the demand of all, less the events queued: those kept for
// some partitions count against the demand of the others.
func (d *partitionDispatch) capacity() int {
	demand := 0
	for _, p := range d.parts {
		demand += p.consumer.demand
	}
	return max(demand-d.queued, 0)
}

func (d *partitionDispatch) held() int { return d.queued }

// trim cuts every queue to a level, the same for all, so that no more
// than size are queued: the longest lose most.
func (d *partitionDispatch) trim(size int, newest bool) int {
	if d.queued <= size {
		return 0
	}
	level := d.level(size)
	dropped := 0
	for k := range d.parts {
		var cut int
		d.parts[k].queue, cut = trimBuffer(d.parts[k].queue, level, newest)
		dropped += cut
	}
	d.queued -= dropped
	return dropped
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
		n += min(lenOf(p.queue), level)
	}
	return n
}

// find returns the partition of the consumer of ref, or -1.
func (d *partitionDispatch) find(ref subRef) int {
	return slices.IndexFunc(d.parts, func(p partition) bool { return p.taken && p.consumer.ref == ref })
}

// flush sends the consumer of partition k its queue, as far as it asked
// for it.
func (d *partitionDispatch) flush(k int, out []actor.Effect) []actor.Effect {
	p := &d.parts[k]
	n := min(p.consumer.demand, lenOf(p.queue))
	if n == 0 {
		return out
	}
	out = append(out, d.send(k, p.queue, n))
	p.queue = p.queue.tail(n)
	d.queued -= n
	return out
}

// send sends the first n events of b to the consumer of partition k,
// taking them from its demand.
func (d *partitionDispatch) send(k int, b batch, n int) actor.Effect {
	c := &d.parts[k].consumer
	c.demand -= n
	return sendEvents(*c, b, n)
}
