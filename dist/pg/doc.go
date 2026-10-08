// Package pg provides process groups, like Erlang's pg: processes join
// groups by name, and others find them, or send to them all, on any node.
//
// A scope is a server holding groups. It is a genserver, started with
// [Start] or [StartLink] under a name, usually under a supervisor. A
// group is named by any comparable value. A process may join a group
// several times, and has to leave it as many times to be out of it.
//
// A process that exits leaves all its groups: the scope monitors its
// members.
//
// # Nodes
//
// A scope started under a molecule.Local name shares its groups with the
// scopes of the same name on the nodes its node is connected to, as
// Erlang's pg does: it monitors the nodes, discovers the scope of each
// node coming up, which tells it its members, then those joining and
// leaving. Members returns the members joined through every scope, those
// of the scope asked first. The members of a node lost, or cut off, are
// dropped, and come back with it.
//
// A scope tells others only of the members joined through it, wherever
// they run. The groups, and the messages of SendEffect, travel between
// nodes as messages of dist: their types are registered with
// dist.Register, as for any message.
//
// # Behaviours
//
// [Join], [Leave], [Members] and [Which] are calls, for code that may wait.
// A behaviour, being pure, does not wait: it returns [JoinEffect],
// [LeaveEffect] and [SendEffect] instead, and asks for members with
// molecule.SendRequest and a [MembersRequest], the answer arriving later.
// SendEffect, which Erlang's pg lacks, has the scope send a message to
// every member of a group, on any node, the way a behaviour broadcasts
// without knowing the members.
//
// The scope is a pure genserver, so gensim can run it, with [Scope], on
// several simulated nodes as well.
package pg
