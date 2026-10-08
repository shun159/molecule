package gen

import (
	"context"
	"log/slog"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

// Starter starts a child linked to its parent, in a process of its own,
// and returns once it has started: what a supervisor starts.
type Starter interface {
	StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error)
}

// Child is a Starter of a behaviour, which a simulation can start as
// well: Simulate returns the runner of the behaviour on env, the
// arguments of its Init, and the options of its start.
type Child interface {
	Starter
	Simulate(env Env) (Runner, any, molecule.StartOptions)
}

// ChildOf returns b, started with args and opts, as a Child.
func ChildOf[S any](b Behaviour[S], args any, opts ...molecule.Option) Child {
	return child[S]{b, args, opts}
}

type child[S any] struct {
	b    Behaviour[S]
	args any
	opts []molecule.Option
}

func (c child[S]) StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error) {
	return StartLink(ctx, parent, c.b, c.args, c.opts...)
}

func (c child[S]) Simulate(env Env) (Runner, any, molecule.StartOptions) {
	return NewRunner(c.b, env), c.args, newOptions(c.opts)
}

// Effects for behaviours that start others, as supervisors.
type (
	// StartChild starts Child linked to the behaviour, at once, waiting
	// for it to start: Started, with Tag, is handled right after the
	// callback, as a ContinueMsg, before anything in the mailbox.
	StartChild struct {
		molecule.Extension
		Child Starter
		Tag   any
	}
	// Log logs a record to the logger of the node, as reports do.
	Log struct {
		molecule.Extension
		Level slog.Level
		Msg   string
		Attrs []any
	}
	// AckLater, among the effects of Init, holds back the end of the
	// start until an Ack: Start returns once the behaviour has started
	// what it starts, as a supervisor its children.
	AckLater struct{ molecule.Extension }
	// Ack ends the start held back by AckLater: Start returns Err, nil
	// for a success. A behaviour failing its start stops as well.
	Ack struct {
		molecule.Extension
		Err error
	}
)

// Started is the outcome of a StartChild.
type Started struct {
	Tag any
	PID proc.PID
	Err error
}

// ParentExiter is a Behaviour handling the exit of its parent itself,
// rather than terminating at once: a supervisor stops its children first.
// It gets the reason, and is expected to stop, in time, with a Stop.
type ParentExiter[S any] interface {
	ParentExit(state S, reason error) (S, []molecule.Effect)
}
