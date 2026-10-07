package gen

import (
	"context"
	"errors"
	"fmt"

	"github.com/shun159/molecule/proc"
)

// Behaviour is a server written as pure functions: each callback gets the
// state and returns the next one along with the effects to perform. The
// runtime started by Start or StartLink owns the process, receives the
// messages and performs the effects.
type Behaviour[S any] interface {
	// Init returns the initial state. An error fails the start: Start
	// returns it and the process exits with it, except ErrIgnore, which
	// makes the process exit normally.
	Init(args any) (S, []Effect, error)
	// Handle handles one message. A panic terminates the behaviour with
	// a proc.PanicError, Terminate getting the state before the message.
	Handle(state S, msg Msg) (S, []Effect)
	// Terminate is called when the behaviour stops through a Stop effect,
	// a panic in Handle, or the exit of its parent while trapping exits.
	// Its effects run, except Stop. It is not called when the process is
	// killed or dies from an exit signal it does not trap.
	Terminate(state S, reason error) []Effect
}

// ErrIgnore, returned by Init, makes Start return it without the process
// failing, like ignore in OTP.
var ErrIgnore = errors.New("gen: ignore")

// AlreadyStartedError is returned by Start when the name is taken.
type AlreadyStartedError struct {
	PID proc.PID
}

func (e *AlreadyStartedError) Error() string {
	return fmt.Sprintf("gen: already started as %v", e.PID)
}

// Option configures Start and StartLink.
type Option func(*options)

type options struct {
	name Name
}

// WithName registers the process under name before Init runs.
func WithName(name Name) Option {
	return func(o *options) { o.name = name }
}

// StartLink starts b in a new process linked to parent and waits until
// Init has run, like gen:start_link. See proc.Self.StartLink for how
// failures and ctx are handled.
func StartLink[S any](ctx context.Context, parent *proc.Self, b Behaviour[S], args any, opts ...Option) (proc.PID, error) {
	o := newOptions(opts)
	return parent.StartLink(ctx, func(s *proc.Self) error { return run(s, b, args, o) })
}

// Start starts b in a new process and waits until Init has run, like
// gen:start. It may be called from outside any process.
func Start[S any](ctx context.Context, n *proc.Node, b Behaviour[S], args any, opts ...Option) (proc.PID, error) {
	o := newOptions(opts)
	return n.Start(ctx, func(s *proc.Self) error { return run(s, b, args, o) })
}

// StartLinkFunc returns a function that starts b linked to its parent, to
// use as the Start of a supervisor.ChildSpec.
func StartLinkFunc[S any](b Behaviour[S], args any, opts ...Option) func(context.Context, *proc.Self) (proc.PID, error) {
	return func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return StartLink(ctx, parent, b, args, opts...)
	}
}

func newOptions(opts []Option) options {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
