// Package proc provides Erlang-style lightweight processes on top of
// goroutines: PIDs, unbounded mailboxes, spawn and exit reasons.
//
// It plays the role of both the ERTS process primitives and proc_lib.
// Higher level behaviours (gen, genserver, supervisor, ...) are built on it.
//
// A process is owned by a single goroutine. The owner gets a *Self, which
// carries the capabilities only the owner may use (Receive, Spawn, ...).
// Anyone may hold a PID, which is plain, comparable data and does not keep
// the process alive.
package proc
