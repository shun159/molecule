package proc

import "fmt"

// Ref is a reference unique among all nodes, like an Erlang reference.
// As with PID, it is plain comparable data so it survives serialization.
type Ref struct {
	node     string
	creation uint32
	id       uint64
}

// IsZero reports whether r is the zero Ref.
func (r Ref) IsZero() bool { return r == Ref{} }

func (r Ref) String() string {
	if r.IsZero() {
		return "#Ref<undefined>"
	}
	return fmt.Sprintf("#Ref<%s.%d.%d>", r.node, r.id, r.creation)
}
