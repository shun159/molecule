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

func (Demand) newDispatch() (dispatch, error)    { return demandDispatch{}, nil }
func (Broadcast) newDispatch() (dispatch, error) { return broadcastDispatch{}, nil }

func (p Partition[T]) newDispatch() (dispatch, error) {
	if p.Partitions <= 0 || p.Hash == nil {
		return nil, ErrNoPartitions
	}
	return partitionDispatch{hash: p.Hash, parts: make([]partition, p.Partitions)}, nil
}

// dispatch is the dispatching of the events of a stage to its consumers:
// a value, each change returning the next.
type dispatch interface {
	// subscribe takes the consumer of m, or tells why not.
	subscribe(m subscribeMsg) (dispatch, error)
	// ask adds n to the demand of the consumer of ref, sending it what
	// waits for it, and reports whether there is that consumer.
	ask(ref subRef, n int) (dispatch, []actor.Effect, bool)
	// leave forgets the consumer of ref, sending the others what waited
	// for it, and reports whether there was that consumer.
	leave(ref subRef) (dispatch, []actor.Effect, bool)
	// route sends the events of b as far as they are asked for, and keeps
	// the rest.
	route(b batch) (dispatch, []actor.Effect, error)
	// capacity is how many more events the stage may emit with none kept
	// beyond the demand.
	capacity() int
	// holds reports whether events are kept.
	holds() bool
}

// downstream is a consumer, and its demand: the events it asked for and
// was not sent.
type downstream struct {
	ref    subRef
	demand int
}

// findConsumer returns the index of the consumer of ref, or -1.
func findConsumer(ds []downstream, ref subRef) int {
	return slices.IndexFunc(ds, func(d downstream) bool { return d.ref == ref })
}

// sendEvents is the effect sending the first n events of b to the
// consumer of ref.
func sendEvents(ref subRef, b batch, n int) actor.Effect {
	return actor.Send{To: ref.Consumer, Msg: eventsMsg{Ref: ref, Events: b.head(n)}}
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
