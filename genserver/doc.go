// Package genserver is the generic server behaviour, like Erlang's
// gen_server: a process holding a state, answering calls and taking casts.
//
// A server is a type implementing [Behaviour]. Its callbacks are pure: each
// gets the state and returns the next one, with the effects of gen for the
// runtime to perform, replies among them. Configuration belongs in the
// fields of the type, which Init reads.
//
// # Callbacks
//
//	Init(self) (S, []molecule.Effect, error)                init/1
//	HandleCall(S, Req, From[Rep]) (S, []molecule.Effect)    handle_call/3
//	HandleCast(S, Cast) (S, []molecule.Effect)              handle_cast/2
//	HandleInfo(S, any) (S, []molecule.Effect)               handle_info/2, optional
//	HandleContinue(S, any) (S, []molecule.Effect)           handle_continue/2, optional
//	Terminate(S, error) []molecule.Effect                   terminate/2, optional
//
// The optional callbacks are those of [InfoHandler], [ContinueHandler] and
// [Terminator]. Without HandleInfo, other messages are dropped. Without
// HandleContinue, a molecule.Continue stops the server with
// [ErrNoHandleContinue].
//
// A server implementing molecule.StatusFormatter formats what the report of
// its terminating tells, like format_status/1: to hide secrets of its
// state, say.
//
// A server embedding [Default] needs no Init starting with the zero
// state, and no HandleCall or HandleCast it has no use for:
//
//	type Log struct{ genserver.Default[[]string] }
//
//	func (Log) HandleInfo(log []string, msg any) ([]string, []molecule.Effect) { ... }
//
// A server takes calls of one type Req and casts of one type Cast; several
// requests are several types implementing one interface, switched on in the
// callback. A request of another type, which only molecule.Call or molecule.SendCast
// can send, stops the server with a [*BadMessageError].
//
// # Replies
//
// HandleCall answers with the effect of [From.Reply], which only takes a
// Rep. A server may also keep the From in its state and reply later, from
// another callback; the caller waits until then, until the server exits, or
// until its context is done.
//
// # Clients
//
// [Start] and [StartLink] return a [Ref], a typed handle on the server with
// Call, Cast and Stop. The type parameters are inferred from the
// behaviour. For a server registered under a name, [RefFor] makes a Ref
// from the name, taking the types from the behaviour:
//
//	counter := genserver.RefFor(Counter{}, molecule.Local("counter"))
//	n, err := counter.Call(ctx, caller, Get{})
//
// [Ref.SendRequest] calls without waiting, like gen_server:send_request:
// the reply is taken from the Pending it returns, in a select on its Done,
// or with Wait.
//
// [StartLinkFunc] makes the start function of a supervisor.ChildSpec.
//
// # Servers calling servers
//
// A callback cannot call another server and wait: it would no longer be a
// pure function. It returns [Ref.CallEffect] instead, and the reply arrives
// later as a molecule.Response in HandleInfo, with the tag given. Two servers
// calling each other therefore cannot deadlock. [Ref.CastEffect] casts.
//
// # Stopping
//
// A server stops when a callback returns molecule.Stop, when a callback panics,
// when [Ref.Stop] or gen.Terminate is called, and, if it traps exits, when
// its parent exits. Terminate runs then. See gen for the details.
//
// # Testing
//
// The callbacks are functions of their arguments, and are tested by calling
// them:
//
//	n, effs := Counter{}.HandleCall(3, Reset{}, from)
//	// n == 0, effs == molecule.Do(from.Reply(3))
package genserver
