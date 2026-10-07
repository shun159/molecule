package proc

import (
	"errors"
	"fmt"
)

// Exit reasons. They are compared with errors.Is, so callers may wrap them
// to carry extra detail, e.g. fmt.Errorf("%w: config reloaded", Shutdown)
// as the equivalent of {shutdown, Term}.
var (
	// Normal is the reason of a process whose function returned nil.
	Normal = errors.New("normal")
	// Shutdown is the reason used for an orderly stop requested by a parent.
	Shutdown = errors.New("shutdown")
	// ErrGoexit is the reason of a process whose goroutine called
	// runtime.Goexit, e.g. through testing.T.FailNow.
	ErrGoexit = errors.New("goexit")
	// Kill, sent with Self.Exit, terminates the target even if it traps
	// exits. The target then dies with reason Killed. Kill arriving through
	// a link (a process that exited with reason Kill) is an ordinary,
	// trappable reason.
	Kill = errors.New("kill")
	// Killed is the reason of a process terminated by Kill.
	Killed = errors.New("killed")
	// NoProc is the reason delivered when linking to a process that does
	// not exist.
	NoProc = errors.New("noproc")
	// NoConnection is the reason delivered when the node of a remote
	// process cannot be reached.
	NoConnection = errors.New("noconnection")
)

// ExitMsg is delivered to the mailbox of a process that traps exits in
// place of an exit signal, like {'EXIT', From, Reason} in Erlang.
type ExitMsg struct {
	From   PID
	Reason error
}

// PanicError is the reason of a process that panicked.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

// Unwrap exposes the panic value when it is itself an error.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}
