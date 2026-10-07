package gen

import "github.com/shun159/molecule/proc"

// Msg is what a Behaviour handles: a CallMsg, a CastMsg, an InfoMsg, or a
// ContinueMsg.
type Msg interface{ msg() }

func (CallMsg) msg()     {}
func (CastMsg) msg()     {}
func (InfoMsg) msg()     {}
func (ContinueMsg) msg() {}

// ContinueMsg carries the Msg of a Continue effect. It is handled right
// after the callback that returned the effect, before any other message.
type ContinueMsg struct {
	Msg any
}

// InfoMsg carries any other message: plain messages sent to the process,
// Down for a Monitor effect, Response for a SendRequest effect, the Msg of
// a StartTimer effect when it fires, and proc.ExitMsg when trapping exits
// (except the one from the parent, which terminates the behaviour).
type InfoMsg struct {
	Msg any
}

// Down reports that a process watched by a Monitor effect died.
type Down struct {
	Tag    any
	PID    proc.PID
	Reason error
}

// Response is the outcome of a SendRequest effect: the reply, or an error
// as Call would return it.
type Response struct {
	Tag   any
	Value any
	Err   error
}
