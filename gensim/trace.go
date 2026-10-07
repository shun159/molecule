package gensim

import (
	"fmt"
	"strings"
	"time"

	"github.com/shun159/molecule/proc"
)

// Kind is the kind of an Event.
type Kind int

const (
	Spawned Kind = iota // a process started: To, with its behaviour as Msg
	Sent                // From sent Msg to To; zero From is the driver
	Handled             // To handled Msg from its mailbox
	Exited              // To exited, with reason Msg
	Fired               // a timer of To fired, with Msg
	Dropped             // Msg from From to To was lost
	Fault               // a fault of the network or a node, told by Msg
)

func (k Kind) String() string {
	return [...]string{"spawn", "send", "handle", "exit", "timer", "drop", "fault"}[k]
}

// Event is an entry of the trace of a simulation.
type Event struct {
	At       time.Time
	Kind     Kind
	From, To proc.PID
	Msg      any
}

func (e Event) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%v %-6v ", e.At.Format("15:04:05.000"), e.Kind)
	if e.Kind == Fault {
		fmt.Fprintf(&b, "%v", e.Msg)
		return b.String()
	}
	if e.Kind == Sent || e.Kind == Dropped {
		from := "driver"
		if !e.From.IsZero() {
			from = e.From.String()
		}
		fmt.Fprintf(&b, "%s -> ", from)
	}
	fmt.Fprintf(&b, "%v %+v", e.To, e.Msg)
	return b.String()
}

// Trace returns what has happened so far, in order.
func (s *Sim) Trace() []Event { return s.trace }

// TraceString returns the trace, an event per line.
func (s *Sim) TraceString() string {
	var b strings.Builder
	for _, e := range s.trace {
		b.WriteString(e.String())
		b.WriteByte('\n')
	}
	return b.String()
}
