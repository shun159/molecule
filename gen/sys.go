package gen

import (
	"context"
	"errors"

	"github.com/shun159/molecule/proc"
)

// sysMsg is a system message, handled by the runtime rather than the
// behaviour, like those of Erlang's sys module.
type sysMsg struct {
	From   From
	Req    sysReq
	Reason error // for sysTerminate
}

type sysReq int

const (
	sysGetState sysReq = iota
	sysSuspend
	sysResume
	sysTerminate
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

// Terminate stops the behaviour at to with reason, Normal if nil, like
// sys:terminate and gen_server:stop: its Terminate callback runs, then it
// exits. It returns once the behaviour is dead, and an error unless it
// exited with reason, e.g. because Terminate panicked.
func Terminate(ctx context.Context, caller Caller, to Dest, reason error) error {
	if reason == nil {
		reason = proc.Normal
	}
	_, err := call(ctx, caller, to, func(f From) any {
		return sysMsg{From: f, Req: sysTerminate, Reason: reason}
	})
	// No reply comes: the exit is the answer.
	var ee *ExitError
	if errors.As(err, &ee) && ee.Reason == reason {
		return nil
	}
	if err == nil {
		err = errors.New("gen: Terminate answered rather than exited")
	}
	return err
}

func sysCall(ctx context.Context, caller Caller, to Dest, req sysReq) (any, error) {
	return call(ctx, caller, to, func(f From) any { return sysMsg{From: f, Req: req} })
}
