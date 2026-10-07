package gen

import "context"

// sysMsg is a system message, handled by the runtime rather than the
// behaviour, like those of Erlang's sys module.
type sysMsg struct {
	From From
	Req  sysReq
}

type sysReq int

const (
	sysGetState sysReq = iota
	sysSuspend
	sysResume
)

// GetState returns the current state of the behaviour at to, like
// sys:get_state.
func GetState(ctx context.Context, caller Caller, to Dest) (any, error) {
	return sysCall(ctx, caller, to, sysGetState)
}

// Suspend makes the behaviour at to stop handling messages, other than
// system messages, until Resume. Messages arriving meanwhile are kept and
// handled in order on Resume.
func Suspend(ctx context.Context, caller Caller, to Dest) error {
	_, err := sysCall(ctx, caller, to, sysSuspend)
	return err
}

// Resume undoes Suspend.
func Resume(ctx context.Context, caller Caller, to Dest) error {
	_, err := sysCall(ctx, caller, to, sysResume)
	return err
}

func sysCall(ctx context.Context, caller Caller, to Dest, req sysReq) (any, error) {
	return call(ctx, caller, to, func(f From) any { return sysMsg{From: f, Req: req} })
}
