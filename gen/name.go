package gen

import "github.com/shun159/molecule/proc"

// Dest is where a request can be sent: a proc.PID, a Local name, or any
// user-defined name, like the {via, Module, Name} of Erlang.
type Dest interface {
	WhereIs(n *proc.Node) (proc.PID, bool)
}

// Name is a Dest a process can be registered under when it is started.
type Name interface {
	Dest
	Register(n *proc.Node, pid proc.PID) error
}

// Local is a name in the node's own registry.
type Local string

// WhereIs looks the name up in the registry of n.
func (l Local) WhereIs(n *proc.Node) (proc.PID, bool) { return n.WhereIs(string(l)) }

// Register registers pid under the name in the registry of n.
func (l Local) Register(n *proc.Node, pid proc.PID) error { return n.Register(string(l), pid) }

func (l Local) String() string { return string(l) }
