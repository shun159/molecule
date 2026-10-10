// Package molecule is the vocabulary of programs built on molecule: what
// they name processes by, call and cast them with, and the effects their
// behaviours return. The behaviours themselves are in genserver,
// genstatem and supervisor, the processes in proc.
//
//	func (w Worker) HandleInfo(s work, msg any) (work, []molecule.Effect) {
//		return s, molecule.Do(
//			molecule.Cast{To: workerName.At(w.Peer), Req: checkpoint{s.count}},
//			molecule.StartTimer{Key: "tick", After: w.Every, Msg: tick{}},
//		)
//	}
//
// # Names
//
// A process is reached by a [Dest]: a proc.PID, a [Local] name in the
// registry of its node, a [Remote] name in the registry of a node that may
// be another, like {Name, Node}, or any [Name] a program implements, like
// {via, Module, Name}. [WithName] registers a behaviour under a name when
// it starts.
//
// # Calls and casts
//
// The Refs of genserver and genstatem call and cast their servers, with
// the types of these. Beneath them, [Call] sends a request and waits for
// the reply; the caller is a *proc.Self, or a *proc.Node for code outside
// processes. Call returns
//
//   - the reply, also when the server exited right after sending it;
//   - an [*ExitError] when the server exits, or does not exist, before
//     replying, with the exit reason, so that errors.Is(err, proc.NoProc)
//     tells a missing server;
//   - ctx.Err() when ctx is done first;
//   - [ErrCallingSelf] when a process calls itself.
//
// [SendCast] sends a request without waiting; a cast to a server that does
// not exist is dropped. [Request] calls without waiting for the reply,
// which a [Pending] holds when it comes.
//
// A caller that stops waiting before the reply, its context done or itself
// dead, tells the server with a [CallAbandoned], as do a Pending cancelled
// and a SendRequest timing out: a server working for a caller may stop.
//
// The server gets a [CallMsg] carrying a [From], and replies with the
// [Reply] effect, or [SendReply] outside behaviours. From is plain data: the
// PID of the caller and the reference of the alias the reply goes to. Only
// the first reply to a call arrives.
//
// # Effects
//
// The callbacks of behaviours are pure: they return effects for their
// runtime to perform, rather than performing them.
//
//	Reply        reply to a call
//	Send         send a message to a process
//	Cast         cast a request to a server
//	Stop         stop once the effects have run, after Terminate
//	Continue     handle a message next, before anything in the mailbox
//	Monitor      monitor a process; a Down with a tag arrives
//	Demonitor    remove a monitor
//	StartTimer   have a message arrive after a time, or at one, under a key
//	CancelTimer  cancel a timer
//	SendRequest  call a server without waiting; a Response with a tag arrives
//	Async        run blocking work in a goroutine; an AsyncResult with a key
//	             arrives
//	CancelAsync  cancel an Async
//	Link         link to a process
//	Unlink       unlink from a process
//	TrapExit     trap exit signals, or stop trapping them
//	MonitorNodes have a proc.NodeUp and a proc.NodeDown arrive, as nodes
//	             connect and go
//
// Effects run in the order returned. An operation whose outcome the
// behaviour needs is named by a tag or a key the behaviour chooses, and its
// outcome arrives later as a message carrying it, a [Down] or a
// [Response]. Tags and keys must be comparable. A timer cancelled or
// replaced does not deliver its message, even if it fired before.
//
// # Starting
//
// An error from Init fails the start, and the process exits with it; with
// [ErrIgnore], the start fails but the process exits normally. A start
// with a name taken fails with an [*AlreadyStartedError]. A behaviour
// implementing [StatusFormatter] formats the report of its terminating; one
// implementing [Clocked] is given the clock of its process.
package molecule
