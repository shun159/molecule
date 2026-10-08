package molecule

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

// At returns the name on node, which may be another:
//
//	Cast{To: Local("worker").At("b@host"), Req: req}
func (l Local) At(node string) Remote { return Remote{Node: node, Name: string(l)} }

// Remote is a name in the registry of a node, which may be another, like
// {Name, Node} in Erlang. It is resolved there: calls, casts, requests and
// monitors go to whichever process has the name when they arrive.
type Remote struct {
	Node string
	Name string
}

// WhereIs looks the name up in the registry of n, if the name is of n.
func (r Remote) WhereIs(n *proc.Node) (proc.PID, bool) {
	if r.Node != n.Name() {
		return proc.PID{}, false
	}
	return n.WhereIs(r.Name)
}

func (r Remote) String() string { return "{" + r.Name + "," + r.Node + "}" }
