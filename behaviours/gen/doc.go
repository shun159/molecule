// Package gen is the runtime of behaviours, like Erlang's gen module:
// it runs a behaviour written as pure functions in a process, performing
// the effects it returns, and handles the system messages of sys.
//
// Programs do not use gen: they use genserver, genstatem and the
// vocabulary of the package molecule. gen is for behaviours built on it,
// as genserver, genstatem and gentcpacceptor are.
//
// # Behaviours
//
// A [Behaviour] is a server written as pure functions. Init returns the
// initial state; Handle gets the state and a message and returns the next
// state; Terminate runs when the server stops. Each returns
// molecule.Effects for the runtime to perform. The runtime started by
// [Start] or [StartLink] owns the process: it receives the messages, calls
// the behaviour, and performs the effects.
//
// The state is passed and returned by value, and the runtime copies it from
// call to call. A large state, such as a big struct, makes every call copy
// it and enlarges the stack of the process; it is better kept small, with
// large data behind a pointer to something never modified, or in a map or
// slice copied when changed. The state given is not to be written, nor a
// message once sent: gensim checks it.
//
// Handle gets a [Msg]: a molecule.CallMsg, a molecule.CastMsg, an
// [InfoMsg] for any other message, or a [ContinueMsg]. An InfoMsg also
// carries the outcome of an effect: a molecule.Down for a Monitor, a
// molecule.Response for a SendRequest, the message of a StartTimer, a
// molecule.AsyncResult for an Async, and a proc.ExitMsg when trapping
// exits, except the one from the parent.
//
// Behaviours built on gen define effects of their own by embedding
// molecule.Extension, and handle them in their adapter before the runtime
// sees the rest; genstatem does so for its actions. A [Performer] is an
// effect that the runtime has perform itself, for what lies outside the
// processes: gentcp sends to a socket this way.
//
// # Starting and stopping
//
// [Start] and [StartLink] start a behaviour and wait until Init has run,
// with the options of molecule, such as molecule.WithName.
//
// A behaviour stops on a Stop effect, on a panic in Handle, when its parent
// exits while it traps exits, and on [Terminate]. In these cases Terminate
// runs first; after a panic, it gets the state before the message. It does
// not run when the process is killed, or terminated by an exit signal it
// does not trap.
//
// # System messages
//
// [GetState], [Suspend], [Resume] and [Terminate] are handled by the
// runtime rather than the behaviour, like the functions of sys. A suspended
// behaviour handles system messages only; the other messages wait, and are
// handled in order on Resume. They are calls of messages of their own,
// made with molecule.CallWith.
//
// # Runners and environments
//
// The meaning of the effects lives in a [Runner], which acts on the world
// only through an [Env]. Start drives a runner with the mailbox of a proc
// process, its Env sending through proc; gensim drives one in a
// simulation, with an Env of its own. A behaviour runs the same code in
// both.
//
// # Reports
//
// A behaviour stopping with an abnormal reason is reported to the logger of
// its node, with the message it was handling and its state, formatted by
// its molecule.StatusFormatter if any, before the crash report of its
// process.
package gen
