package molecule

import (
	"errors"
	"fmt"

	"github.com/shun159/molecule/proc"
)

// Status is what the report of a behaviour terminating tells: the state,
// the message it was handling, and why it terminates.
type Status struct {
	State   any
	Message any
	Reason  error
}

// StatusFormatter is a Behaviour that formats its Status for reports,
// like format_status/1: to hide what is not to be logged, as secrets, or
// to shorten what is large. It may replace any of the fields with any
// value. The state the behaviour runs with is not changed.
type StatusFormatter interface {
	FormatStatus(Status) Status
}

// ErrIgnore, returned by Init, makes Start return it without the process
// failing, like ignore in OTP.
var ErrIgnore = errors.New("molecule: ignore")

// AlreadyStartedError is returned by Start when the name is taken.
type AlreadyStartedError struct {
	PID proc.PID
}

func (e *AlreadyStartedError) Error() string {
	return fmt.Sprintf("molecule: already started as %v", e.PID)
}

// Option configures the start of a behaviour.
type Option func(*StartOptions)

// StartOptions are the options of a start, as the runtime of behaviours
// reads them.
type StartOptions struct {
	Name Name
}

// WithName registers the process under name before Init runs.
func WithName(name Name) Option {
	return func(o *StartOptions) { o.Name = name }
}
