// Package pg provides process groups, like Erlang's pg: processes join
// groups by name, and others find them, or send to them all.
//
// A scope is a server holding groups. It is a genserver, started with
// [Start] or [StartLink] under a name, usually under a supervisor. A
// group is named by any comparable value. A process may join a group
// several times, and has to leave it as many times to be out of it.
//
// A process that exits leaves all its groups: the scope monitors its
// members.
//
// # Behaviours
//
// [Join], [Leave], [Members] and [Which] are calls, for code that may wait.
// A behaviour, being pure, does not wait: it returns [JoinEffect],
// [LeaveEffect] and [SendEffect] instead, and asks for members with
// molecule.SendRequest and a [MembersRequest], the answer arriving later. SendEffect, which Erlang's pg
// lacks, has the scope send a message to every member of a group, the way
// a behaviour broadcasts without knowing the members.
//
// The scope is a pure genserver, so gensim can run it, with [Scope].
package pg
