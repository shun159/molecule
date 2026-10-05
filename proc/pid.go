package proc

import "fmt"

// PID identifies a process, local or remote.
//
// Like an Erlang pid, it is pure data: the node name, the node's creation
// (incarnation) and a per-node serial. It holds no pointer to the process,
// so it is comparable, usable as a map key and can be serialized for
// distribution without losing identity. A PID whose node matches but whose
// creation differs refers to a process of a previous incarnation of the node
// and is never alive.
type PID struct {
	node     string
	creation uint32
	id       uint64
}

// Node returns the name of the node the process belongs to.
func (p PID) Node() string { return p.node }

// IsZero reports whether p is the zero PID, which refers to no process.
func (p PID) IsZero() bool { return p == PID{} }

func (p PID) String() string {
	if p.IsZero() {
		return "<undefined>"
	}
	return fmt.Sprintf("<%s.%d.%d>", p.node, p.id, p.creation)
}
