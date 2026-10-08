// Package gen is what behaviours have in common, like Erlang's gen module:
// calling a process and replying to it, the pure behaviour and the runtime
// performing its effects, and the system messages of sys.
//
// genserver and genstatem are built on gen. A [Behaviour] can also be
// written against gen directly, handling every kind of message itself.
//
// # Calls and casts
//
// [Call] sends a request to a server and waits for the reply. The server is
// given as a [Dest]: a proc.PID, a [Local] name, or any [Name] implemented
// by the user, like {via, Module, Name} in Erlang. The caller is a
// *proc.Self, or a *proc.Node for code outside processes. Call returns
//
//   - the reply, also when the server exited right after sending it;
//   - an [*ExitError] when the server exits, or does not exist, before
//     replying, with the exit reason, so that errors.Is(err, proc.NoProc)
//     tells a missing server;
//   - ctx.Err() when ctx is done first;
//   - [ErrCallingSelf] when a process calls itself.
//
// [SendCast] sends a request without waiting. A cast to a server that does
// not exist is dropped. [Request] calls without waiting for the reply,
// which a [Pending] holds when it comes.
//
// genserver and genstatem wrap these with the types of their servers;
// programs use theirs.
//
// The server gets a [CallMsg] carrying a [From], and replies with the
// [Reply] effect, or [SendReply] outside behaviours. From is plain data: the
// PID of the caller and the reference of the alias the reply goes to. Only
// the first reply to a call arrives.
//
// # Behaviours
//
// A [Behaviour] is a server written as pure functions. Init returns the
// initial state; Handle gets the state and a message and returns the next
// state; Terminate runs when the server stops. Each returns effects for the
// runtime to perform, rather than performing them. The runtime started by
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
// Handle gets a [Msg]: a [CallMsg], a [CastMsg], an [InfoMsg] for any other
// message, or a [ContinueMsg]. An InfoMsg also carries the outcome of an
// effect: a [Down] for a Monitor, a [Response] for a SendRequest, the
// message of a StartTimer, and a proc.ExitMsg when trapping exits, except
// the one from the parent.
//
// # Effects
//
//	Reply        reply to a call
//	Send         send a message to a process
//	Cast         cast a request to a server
//	Stop         stop once the effects have run, after Terminate
//	Continue     handle a message next, before anything in the mailbox
//	Monitor      monitor a process; a Down with a tag arrives
//	Demonitor    remove a monitor
//	StartTimer   have a message arrive after a time, under a key
//	CancelTimer  cancel a timer
//	SendRequest  call a server without waiting; a Response with a tag arrives
//	Link         link to a process
//	Unlink       unlink from a process
//	TrapExit     trap exit signals, or stop trapping them
//
// Effects run in the order returned. An operation whose outcome the
// behaviour needs is named by a tag or a key the behaviour chooses, and its
// outcome arrives later as an InfoMsg carrying it. Tags and keys must be
// comparable. A timer cancelled or replaced does not deliver its message,
// even if it fired before.
//
// Behaviours built on gen define effects of their own by embedding
// [Extension], and handle them in their adapter before the runtime sees the
// rest. genstatem does so for its actions. A [Performer] is an effect that
// the runtime has perform itself, for what lies outside the processes:
// gentcp sends to a socket this way.
//
// # Starting and stopping
//
// [Start] and [StartLink] start a behaviour and wait until Init has run. An
// error from Init fails the start, and the process exits with it; with
// [ErrIgnore], the start fails but the process exits normally. With
// [WithName], the process is registered before Init, and the start fails
// with an [*AlreadyStartedError] if the name is taken.
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
// handled in order on Resume.
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
// its node, with the message it was handling and its state, before the
// crash report of its process.
package gen
