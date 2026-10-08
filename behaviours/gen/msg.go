package gen

// Msg is what a Behaviour handles: a molecule.CallMsg, a
// molecule.CastMsg, an InfoMsg, or a ContinueMsg.
type Msg = any

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
