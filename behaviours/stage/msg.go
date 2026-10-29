package stage

import "github.com/shun159/molecule/proc"

// The messages between a producer and its consumers, like those of
// GenStage: a consumer subscribes asking for events and asks for more as
// it handles them; the producer sends no more than asked; either ends the
// subscription.

// subRef names a subscription: its consumer, and the count of the
// subscriptions it made before.
type subRef struct {
	Consumer proc.PID
	N        uint64
}

type (
	// subscribeMsg subscribes, asking for Ask events, to Partition of a
	// Partition.
	subscribeMsg struct {
		Ref       subRef
		Ask       int
		Partition int
	}
	// askMsg asks for N more events.
	askMsg struct {
		Ref subRef
		N   int
	}
	// eventsMsg carries events, the events[T] of the type the producer
	// emits.
	eventsMsg struct {
		Ref    subRef
		Events batch
	}
	// cancelMsg is a consumer cancelling.
	cancelMsg struct {
		Ref    subRef
		Reason error
	}
	// cancelledMsg is a producer cancelling.
	cancelledMsg struct {
		Ref    subRef
		Reason error
	}
)

// The tags of the monitors of each end of a subscription on the other.
type (
	producerDown struct{ ref subRef }
	consumerDown struct{ ref subRef }
)
