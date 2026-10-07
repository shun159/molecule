// Package gensim runs behaviours in a deterministic simulation: one
// goroutine, a virtual clock, and an order of events decided by a seed.
//
// The behaviours run on the same runtime as in a real process, gen.Runner,
// with a simulated gen.Env instead of a proc one: what is tested is what
// runs. genserver.Gen and genstatem.Gen turn their behaviours into the
// gen.Behaviour Spawn takes.
//
//	s := gensim.New(seed)
//	pid, _ := gensim.Spawn(s, genserver.Gen(Counter{}), nil)
//	s.Cast(pid, Add{1})
//	v, err := s.Call(pid, Get{})
//
// # Order of events
//
// A message sent is in flight until a step delivers it to the mailbox of
// its receiver; another step has a process handle the first message of its
// mailbox. Each step is chosen among all the possible ones by the seed.
// Messages from one process to another arrive in the order sent, as in
// Erlang; messages from different processes interleave in any order. The
// same seed with the same calls gives the same run, so a failure found
// with one seed is replayed by running that seed again, and Trace tells
// what happened.
//
// # Time
//
// Time is virtual and passes only when nothing else can happen: then the
// next timer fires, and the clock moves to it. Call lets time pass while
// it waits, up to CallTimeout; Advance makes it pass.
//
// # Faults
//
// Exit sends an exit signal to a process, proc.Kill killing it. Links,
// monitors, exit signals and trapping exits work as in proc.
//
// # Scope
//
// Processes are behaviours of gen. Supervisors and gentcpacceptor, which
// are not, cannot be simulated, nor processes written against proc
// directly. Names are gen.Local names; other gen.Name implementations do
// not resolve.
package gensim
