package molecule

import (
	"time"

	"github.com/shun159/molecule/proc"
)

// Effect is something a Behaviour asks the runtime to do. Behaviours stay
// pure by returning effects instead of performing them; effects run in the
// order returned. Operations whose outcome the behaviour needs (monitors,
// timers, requests) are identified by a tag or key chosen by the behaviour,
// and their outcome arrives later as an InfoMsg carrying it. Tags and keys
// must be comparable.
type Effect interface{ effect() }

// Do collects effects, for brevity: return s, Do(a, b).
func Do(effs ...Effect) []Effect { return effs }

// Reply replies to a call.
type Reply struct {
	To    From
	Value any
}

// Send sends Msg to To as a plain message.
type Send struct {
	To  Dest
	Msg any
}

// Cast casts Req to the server at To.
type Cast struct {
	To  Dest
	Req any
}

// Continue makes Msg the next message handled, as a ContinueMsg, before
// any other in the mailbox, like {continue, Msg} in OTP. It splits work
// in steps, such as initialization to finish once Start has returned.
// Several are handled in the order returned. A Stop drops them.
type Continue struct {
	Msg any
}

// Stop terminates the behaviour once the remaining effects have run:
// Terminate is called, then the process exits with Reason (Normal if nil).
type Stop struct {
	Reason error
}

// Monitor watches Target. When it dies, a Down with Tag arrives. A Target
// that does not exist yields a Down with proc.NoProc at once. Monitoring
// under a Tag already in use replaces that monitor.
type Monitor struct {
	Target Dest
	Tag    any
}

// Demonitor removes the monitor under Tag. No Down for it arrives
// afterwards.
type Demonitor struct {
	Tag any
}

// StartTimer makes Msg arrive as an InfoMsg after After. Starting a timer
// under a Key already in use replaces it.
type StartTimer struct {
	Key   any
	After time.Duration
	Msg   any
	// At, if set, is when the timer fires rather than After: a time in
	// the past fires at once.
	At time.Time
}

// CancelTimer cancels the timer under Key. Its Msg does not arrive
// afterwards.
type CancelTimer struct {
	Key any
}

// SendRequest calls To without waiting, like gen_server:send_request. The
// outcome arrives as a Response with Tag. A positive Timeout bounds the
// wait, ending it with context.DeadlineExceeded.
type SendRequest struct {
	To      Dest
	Req     any
	Tag     any
	Timeout time.Duration
}

// Link links the process with PID.
type Link struct {
	PID proc.PID
}

// Unlink removes the link with PID.
type Unlink struct {
	PID proc.PID
}

// TrapExit sets whether exit signals arrive as proc.ExitMsg. A behaviour
// trapping exits is terminated through Terminate when its parent exits.
type TrapExit struct {
	On bool
}

// MonitorNodes makes the process monitor the connections of its node to
// others, or stop, like net_kernel:monitor_nodes: a proc.NodeUp arrives
// at once for each node connected, then a proc.NodeUp and a
// proc.NodeDown each time a connection is made and lost.
type MonitorNodes struct {
	On bool
}

// Extension is embedded in the effects of behaviours built on gen, such as
// the actions of genstatem: an effect type embedding it is an Effect, for
// the adapter of the behaviour to handle before returning the rest to the
// runtime, which panics on any it is given, but a gen.Performer.
type Extension struct{}

func (Extension) effect() {}

func (Continue) effect()     {}
func (Reply) effect()        {}
func (Send) effect()         {}
func (Cast) effect()         {}
func (Stop) effect()         {}
func (Monitor) effect()      {}
func (Demonitor) effect()    {}
func (StartTimer) effect()   {}
func (CancelTimer) effect()  {}
func (SendRequest) effect()  {}
func (Link) effect()         {}
func (Unlink) effect()       {}
func (TrapExit) effect()     {}
func (MonitorNodes) effect() {}
