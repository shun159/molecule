package gen

import (
	"log/slog"
	"time"

	"github.com/shun159/molecule/proc"
)

// Env is what a Runner acts on: the process it runs in, and the world
// around it. The runtime of Start provides one made of a proc process;
// gensim provides a simulated one.
//
// Messages an Env delivers to the runner, it delivers through the mailbox
// of Self, for the driver of the runner to hand to Deliver.
type Env interface {
	// Self is the PID of the process, Parent that of its parent, if any.
	Self() proc.PID
	Parent() proc.PID
	// Resolve finds the process at dest.
	Resolve(dest Dest) (proc.PID, bool)
	// Send sends msg to the process to.
	Send(to proc.PID, msg any)
	// SendName sends msg to the process registered as name on node.
	SendName(node, name string, msg any)
	// SendAlias sends msg to the alias ref, a reply to a call.
	SendAlias(ref proc.Ref, msg any)
	// Monitor monitors pid; a proc.DownMsg with the reference arrives
	// when it exits. Demonitor removes the monitor and its DownMsg.
	Monitor(pid proc.PID) proc.Ref
	Demonitor(ref proc.Ref)
	// MonitorName monitors the process registered as name on node, as
	// proc.Self.MonitorName does.
	MonitorName(node, name string) proc.Ref
	// Link, Unlink and TrapExit are those of proc.Self.
	Link(pid proc.PID)
	Unlink(pid proc.PID)
	TrapExit(on bool)
	// Now returns the time, of the clock SendAfter runs on.
	Now() time.Time
	// SendAfter sends msg to Self after d, unless cancel is called first.
	SendAfter(d time.Duration, msg any) (cancel func())
	// Request makes an alias that also monitors pid, as
	// proc.Node.MonitorAlias does: the first of the message sent to the
	// alias and the exit of pid arrives, as reply(ref, m). release
	// deactivates the alias; nothing arrives after it returns.
	Request(pid proc.PID, reply func(ref proc.Ref, m proc.AliasMsg) any) (ref proc.Ref, release func())
	// RequestName is Request for the process registered as name on node,
	// monitored there, as proc.Node.MonitorAliasName does.
	RequestName(node, name string, reply func(ref proc.Ref, m proc.AliasMsg) any) (ref proc.Ref, release func())
	// Logger is where reports go.
	Logger() *slog.Logger
}

// Runner runs a Behaviour against an Env: it handles the messages a driver
// gives it, and performs the effects through the Env. The runtime of Start
// is one driver, feeding it the mailbox of a process; gensim is another.
type Runner interface {
	// Init runs the Init of the behaviour and its effects. An error fails
	// the start, as described for Behaviour.Init; a Stop among the effects
	// fails it with the reason of the Stop. Continue messages are left for
	// Flush.
	Init(args any) error
	// Flush handles the pending Continue messages, then the messages kept
	// during a suspension that has ended. It reports whether the behaviour
	// has terminated, and its exit reason if so.
	Flush() (done bool, reason error)
	// Deliver handles msg, received from the mailbox, then flushes.
	Deliver(msg any) (done bool, reason error)
	// Abort releases what the runner holds, timers and requests, when the
	// process dies without terminating, killed.
	Abort()
	// State returns the state of the behaviour.
	State() any
}

// NewRunner returns a runner of b acting on env.
func NewRunner[S any](b Behaviour[S], env Env) Runner {
	return newRuntime(b, env)
}
