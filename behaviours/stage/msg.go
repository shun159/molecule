package stage

import "github.com/shun159/molecule/proc"

// The messages between a producer and its consumers, like those of
// GenStage: a consumer subscribes asking for events and asks for more as
// it handles them; the producer sends no more than asked; either cancels.

// subRef names a subscription: its consumer, and the count of the
// subscriptions it made before.
type subRef struct {
	Consumer proc.PID
	N        uint64
}

type (
	// subscribeMsg subscribes, asking for Ask events.
	subscribeMsg struct {
		Ref subRef
		Ask int
	}
	// askMsg asks for N more events.
	askMsg struct {
		Ref subRef
		N   int
	}
	// eventsMsg carries events, a slice of the type the producer emits.
	eventsMsg struct {
		Ref    subRef
		Events any
	}
	// cancelMsg ends a subscription, either way.
	cancelMsg struct {
		Ref    subRef
		Reason error
	}
)

// The tags of the monitors of each end of a subscription on the other.
type (
	producerDown struct{ ref subRef }
	consumerDown struct{ ref subRef }
)
