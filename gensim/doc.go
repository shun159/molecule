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
// # Nodes
//
// Processes run on DefaultNode, or on the node given with On: nodes are
// made on first use, each with names of its own, which gen.Remote reaches
// from the others. Nodes fail as they do with dist, at the steps the test
// chooses, the order of all else decided by the seed:
//
//	Partition  cuts groups of nodes from each other; Heal undoes it
//	Crash      stops a node: its processes vanish, without Terminate
//	Restart    starts it again, a new incarnation, for the test to spawn on
//	Loss       loses messages between nodes, at random, beyond Erlang
//
// When two nodes lose each other, what is in flight between them is lost,
// and links, monitors and pending requests between them fail with
// proc.NoConnection, on both sides.
//
// # Purity
//
// A behaviour must not write the state it is given, nor a message once
// sent; a slice or a map shared by the state given and the state returned
// is how it usually happens, unnoticed, as the program still runs. The
// simulation fingerprints the state a process is given, and each message
// in flight, through their slices, maps and pointers, and panics with an
// [ImpureError] when one has changed: at the step that did it, replayed by
// its seed. WithoutPurityCheck turns this off.
//
// # Randomness
//
// A behaviour needing random numbers, for a timeout or a choice of peer,
// keeps its generator in its state: a rand.PCG is a value, advanced by
// calling it on the copy the callback returns. Seeded from the arguments
// of Init, it makes the same numbers in every run of the same seed.
//
// # Scope
//
// Processes are behaviours of gen. Supervisors and gentcpacceptor, which
// are not, cannot be simulated, nor processes written against proc
// directly. Names are gen.Local and gen.Remote names; other gen.Name
// implementations do not resolve.
package gensim
