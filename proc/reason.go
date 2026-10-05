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
)

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
