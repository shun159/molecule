package molecule

import (
	"errors"
	"fmt"
	"time"

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

// Clocked is a Behaviour that reads the clock: the time a callback is
// handling an event at, to compare against a deadline or to stamp an entry
// of its state. Before its Init, the runtime gives it the clock of its
// process -- time.Now in a process, the virtual clock in gensim, so that a
// simulation runs on simulated time -- and runs the behaviour WithClock
// returns, which must be of the same Behaviour type, in its place.
//
//	type Cache struct {
//		genserver.Default[entries]
//		now func() time.Time
//	}
//
//	func (c Cache) WithClock(now func() time.Time) any { c.now = now; return c }
//
// The clock is read, as in Erlang a callback reads erlang:monotonic_time:
// waiting, rather, is a timer's, StartTimer's At an absolute time.
type Clocked interface {
	WithClock(now func() time.Time) any
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
	Name        Name
	MaxMessages int
	MaxAsync    int
}

// WithName registers the process under name before Init runs.
func WithName(name Name) Option {
	return func(o *StartOptions) { o.Name = name }
}

// WithMaxMessages kills the process once more than n messages wait in its
// mailbox, with reason proc.MailboxFull, for its supervisor to restart it;
// see proc.Self.SetMaxMessages.
func WithMaxMessages(n int) Option {
	return func(o *StartOptions) { o.MaxMessages = n }
}

// WithMaxAsync bounds the asyncs of a behaviour running at once to n: an
// Async started past it waits until one running ends, then runs, in the
// order started, as Task.async_stream with max_concurrency does. A waiting
// async is cancelled as a running one is; its Run then never runs.
func WithMaxAsync(n int) Option {
	return func(o *StartOptions) { o.MaxAsync = n }
}
