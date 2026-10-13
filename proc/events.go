package proc

// A node tells what happens to its processes to a handler of events, see
// WithEvents, for a program to count them, as metrics: a process crashing,
// one killed for its mailbox. Behaviours and calls tell theirs through it
// as well, see Node.Emit. The events are plain structs, of the package
// telling them; the handler tells them apart by their types.

// Crashed is the event of a process whose function failed: the one a crash
// report logs, of an abnormal exit of its own, not of a kill or an exit
// signal.
type Crashed struct {
	PID    PID
	Name   string // registered, if any
	Label  string // see Self.SetLabel
	Reason error
}

// Overflowed is the event of a process killed for its mailbox, more than
// its limit waiting, see Self.SetMaxMessages.
type Overflowed struct {
	PID   PID
	Name  string
	Label string
	Len   int // messages waiting
}

// WithEvents makes the node hand its events to h. h is called in the
// goroutine where the event happens, as it happens, from any process at
// once: it must be safe for concurrent use, and quick, not blocking, as a
// counter is. A panic in h is logged, and ends nothing.
func WithEvents(h func(event any)) Option {
	return func(n *Node) { n.events = h }
}

// Emit hands event to the handler of the events of n, if any. Behaviours
// and calls emit theirs through it.
func (n *Node) Emit(event any) {
	if n.events == nil {
		return
	}
	defer func() {
		if v := recover(); v != nil {
			n.Logger().Error("events: handler panicked", "event", event, "panic", v)
		}
	}()
	n.events(event)
}
