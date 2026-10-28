package stage

import (
	"errors"
	"slices"

	"github.com/shun159/molecule/actor"
)

// Dispatcher is how a producer, or a producer-consumer, sends its events
// to its consumers: Demand, the default, Broadcast or Partition. A stage
// implementing Dispatching has another than Demand.
type Dispatcher interface {
	newDispatch() (dispatch, error)
}

// Dispatching is a producer or a producer-consumer telling its
// Dispatcher; nil is Demand.
type Dispatching interface {
	Dispatcher() Dispatcher
}

// Demand sends each event to one consumer, the one wanting most, as
// GenStage's DemandDispatcher: the consumers share the work, each as fast
// as it goes.
type Demand struct{}

// Broadcast sends each event to every consumer, as GenStage's
// BroadcastDispatcher: as fast as the slowest asks. A consumer gets the
// events emitted once it has subscribed; the events emitted while there
// is none wait for the first.
type Broadcast struct{}

// Partition sends each event to the consumer of its partition, Hash of
// it, from 0 to Partitions-1, as GenStage's PartitionDispatcher: a
// consumer takes a partition with Subscribe, one consumer each. Hash
// drops an event by returning a negative partition, and stops the stage
// with ErrBadPartition by returning one past the last. T is the type of
// the events.
//
// The events of a partition wait for its consumer to want them, and the
// stage is told of less demand while they do: it holds no more events
// than its consumers asked for, and a partition without a consumer, or a
// slow one, holds the others back once they are as many.
type Partition[T any] struct {
	Partitions int
	Hash       func(T) int
}

// Errors of partitions.
var (
	ErrBadPartition   = errors.New("stage: no such partition")
	ErrPartitionTaken = errors.New("stage: the partition has a consumer already")
	ErrNoPartitions   = errors.New("stage: Partition wants Partitions > 0 and a Hash")
)

func (Demand) newDispatch() (dispatch, error)    { return &demandDispatch{}, nil }
func (Broadcast) newDispatch() (dispatch, error) { return &broadcastDispatch{}, nil }

func (p Partition[T]) newDispatch() (dispatch, error) {
	if p.Partitions <= 0 || p.Hash == nil {
		return nil, ErrNoPartitions
	}
	return &partitionDispatch{hash: p.Hash, parts: make([]partition, p.Partitions)}, nil
}

// dispatch is the dispatching of the events of a stage to its consumers.
// It is part of the state of the stage, which a callback must not write:
// it is changed in a clone, made once in the handling of a message, see
// emitting.change. The effects of its methods are appended to out.
type dispatch interface {
	// clone returns a copy to change.
	clone() dispatch
	// subscribe takes the consumer of m, or tells why not.
	subscribe(m subscribeMsg) error
	// ask adds n to the demand of the consumer of ref, sending it what
	// waits for it, and reports whether there is that consumer.
	ask(ref subRef, n int, out []actor.Effect) ([]actor.Effect, bool)
	// leave forgets the consumer of ref, sending the others what waited
	// for it, and reports whether there was that consumer.
	leave(ref subRef, out []actor.Effect) ([]actor.Effect, bool)
	// route sends the events of b as far as they are asked for, and keeps
	// the rest.
	route(b batch, out []actor.Effect) ([]actor.Effect, error)
	// capacity is how many more events the stage may emit with none kept
	// beyond the demand.
	capacity() int
	// held is how many events are kept.
	held() int
	// trim drops events kept beyond size, the newest if newest, else the
	// oldest, and returns how many.
	trim(size int, newest bool) int
}

// downstream is a consumer, and its demand: the events it asked for and
// was not sent.
type downstream struct {
	ref    subRef
	to     actor.Dest // ref.Consumer, made a Dest once, rather than at each send
	demand int
}

// newDownstream returns the consumer of ref, wanting nothing yet.
func newDownstream(ref subRef) downstream {
	return downstream{ref: ref, to: ref.Consumer}
}

// findConsumer returns the index of the consumer of ref, or -1.
func findConsumer(ds []downstream, ref subRef) int {
	return slices.IndexFunc(ds, func(d downstream) bool { return d.ref == ref })
}

// sendEvents is the effect sending the first n events of b to the
// consumer d: b itself if all.
func sendEvents(d downstream, b batch, n int) actor.Effect {
	if n < b.len() {
		b = b.first(n)
	}
	return actor.Send{To: d.to, Msg: eventsMsg{Ref: d.ref, Events: b}}
}

// lenOf is the length of b, 0 if nil.
func lenOf(b batch) int {
	if b == nil {
		return 0
	}
	return b.len()
}

// trimBuffer drops the events of buffer beyond size, the newest if
// newest, else the oldest, and returns how many.
func trimBuffer(buffer batch, size int, newest bool) (batch, int) {
	if buffer == nil || buffer.len() <= size {
		return buffer, 0
	}
	excess := buffer.len() - size
	if newest {
		return buffer.first(size), excess
	}
	return buffer.tail(excess), excess
}

// keep returns buffer with b after it, either nil.
func keep(buffer, b batch) batch {
	switch {
	case b == nil:
		return buffer
	case buffer == nil:
		return b
	}
	return buffer.join(b)
}
