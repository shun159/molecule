package molecule

import "github.com/shun159/molecule/proc"

// From identifies a pending call, to reply to with Reply. It is plain data:
// the caller's PID (zero when the caller is not a process) and the alias
// the reply goes to.
type From struct {
	PID proc.PID
	Tag proc.Ref
}

// CallMsg is the message a server receives for Call. Behaviours get it
// as their Msg.
type CallMsg struct {
	From From
	Req  any
}

// CastMsg is the message a server receives for Cast.
type CastMsg struct {
	Req any
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

// AsyncResult is the outcome of an Async effect: what its Run returned.
type AsyncResult struct {
	Key   any
	Value any
	Err   error
}
