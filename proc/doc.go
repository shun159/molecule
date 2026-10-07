// Package proc provides processes in the manner of Erlang: each runs on its
// own, communicates by messages, and fails on its own. It plays the part of
// the process primitives of ERTS and of proc_lib; the behaviours in gen,
// genserver, genstatem and supervisor are built on it.
//
// # Nodes and processes
//
// A [Node] owns a set of processes, as an Erlang node does. Several nodes
// may live in one program. Processes reach only the processes of their own
// node for now; distribution is not implemented.
//
// A process runs a function in a goroutine of its own:
//
//	pid := n.Spawn(func(s *proc.Self) error {
//		for {
//			msg, err := s.Receive(context.Background())
//			if err != nil {
//				return err
//			}
//			...
//		}
//	})
//
// The function gets a [Self], through which the process receives, sends,
// links, monitors and spawns. A Self belongs to the goroutine of its
// process and is not to be handed to other goroutines.
//
// A [PID] identifies a process. It is plain, comparable data: the node
// name, the creation of the node, and a serial number. Holding a PID does
// not keep its process alive. A PID of a dead process, of a previous
// incarnation of the node, or of another node refers to no process here.
//
// # Messages
//
// [Node.Send] and [Self.Send] put a message at the end of the mailbox of a
// process. Sending never blocks and never fails: a message to a process
// that does not exist is dropped. Messages from one process to another
// arrive in the order they were sent.
//
// Mailboxes are unbounded FIFO queues. A process that receives slower than
// it is sent to has its mailbox grow without limit; a protocol that needs
// flow control asks for data, as gentcpacceptor does. There is no selective
// receive: [Self.Receive] returns the oldest message.
//
// # Exit reasons
//
// A process exits when its function returns: with reason [Normal] for nil,
// and with the error otherwise. On a panic, the reason is a [*PanicError]
// with the value and the stack.
//
// [Shutdown] is the reason of an orderly stop, asked for by a parent. An
// error wrapping Normal or Shutdown counts as such; this is how
// {shutdown, Term} is written. Any other reason is abnormal, see
// [IsAbnormal].
//
// # Links and exit signals
//
// [Self.Link] links two processes. When one exits, the other gets an exit
// signal with the reason:
//
//   - With reason Normal, the signal is ignored.
//   - With any other reason, the receiver exits with that same reason.
//   - A receiver trapping exits, see [Self.TrapExit], gets an [ExitMsg] in
//     its mailbox instead, whatever the reason.
//
// [Self.SpawnLink] links to the new process before it runs. Linking to a
// process that does not exist makes the caller get an exit signal with
// reason [NoProc].
//
// [Self.Exit] sends an exit signal without a link. With reason [Kill] it
// terminates the receiver even if it traps exits, and the receiver exits
// with [Killed]. Through a link, Kill is an ordinary reason.
//
// An exit signal terminating its receiver takes effect at once. The others
// arrive as ExitMsg after the messages sent earlier by the same process.
//
// # Monitors and aliases
//
// [Self.Monitor] watches a process without being affected by its exit: a
// [DownMsg] with the reason arrives in the mailbox when it exits.
// [Self.Demonitor] removes a monitor, and a DownMsg of it already queued.
//
// Code outside processes, or a process waiting on something else than its
// mailbox, watches a process with [Node.Watch], which returns a context
// cancelled with the exit reason.
//
// An [Alias] is a one-shot address, which code outside processes can
// receive from. [Node.MonitorAlias] makes one that also monitors a process:
// either a reply sent to it or the exit of the process arrives, whichever
// is first. Calls in gen are made of these.
//
// # Names
//
// A process may be registered under a name, see [Node.Register]. A process
// has one name at most. It loses its name when it exits, before its links
// and monitors are told, so that another process can take the name as soon
// as the exit is known.
//
// # Synchronous start
//
// [Self.StartLink] and [Node.Start] start a process and wait until it calls
// [Self.InitAck], as proc_lib:start_link does. The process tells there
// whether it started. If it exits first, StartLink returns its exit reason.
// Behaviours and supervisors start this way, so that a supervision tree has
// started once the call starting its top returns.
//
// # Killing
//
// A goroutine cannot be stopped from outside. A process killed, or
// terminated by an exit signal, is dead at once to the others: its links
// and monitors are told, its name is freed, and messages to it are dropped.
// Its goroutine runs on until it next calls [Self.Receive], which returns
// the exit reason. A blocking call a process makes, such as a read from a
// connection, does not return because the process was killed; it must be
// tied to [Self.Context], which is cancelled when the process dies.
//
// # Reports
//
// A process whose function ends with an abnormal reason is reported to the
// logger of its node, see [WithLogger], with its name, parent, initial
// call, and the stack of a panic. A process killed or terminated by an exit
// signal is not reported: no code of its own failed.
package proc
