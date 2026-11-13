package supervisor

import (
	"context"
	"errors"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// DynamicSpec describes a dynamic supervisor, which starts with no
// children and starts them on demand with StartChild, like Elixir's
// DynamicSupervisor or OTP's simple_one_for_one. Its children are
// independent: each is restarted on its own, as with OneForOne, and they
// are all stopped at once rather than in order.
type DynamicSpec struct {
	// Name, if set, is registered when the supervisor starts.
	Name actor.Name
	// Intensity and Period limit restarts as for Spec.
	Intensity int
	Period    time.Duration
	// MaxChildren, if positive, caps the number of children.
	MaxChildren int
}

var (
	// ErrMaxChildren is returned by StartChild when the supervisor has
	// MaxChildren children already.
	ErrMaxChildren = errors.New("supervisor: maximum number of children reached")
	// ErrNotFound is returned by TerminateChild for a PID that is not a
	// child of the supervisor.
	ErrNotFound = errors.New("supervisor: no such child")
)

// StartDynamicLink starts a dynamic supervisor linked to parent.
func StartDynamicLink(ctx context.Context, parent *proc.Self, spec DynamicSpec) (proc.PID, error) {
	return gen.StartLink(ctx, parent, dynamicOf(spec), nil, nameOption(spec.Name)...)
}

// StartDynamic starts a dynamic supervisor, like StartDynamicLink but
// without a link.
func StartDynamic(ctx context.Context, n *proc.Node, spec DynamicSpec) (proc.PID, error) {
	return gen.Start(ctx, n, dynamicOf(spec), nil, nameOption(spec.Name)...)
}

// DynamicChild returns the Starter of a dynamic supervisor, to nest it
// under another supervisor, which gensim can simulate as well.
func DynamicChild(spec DynamicSpec) gen.Child {
	return gen.ChildOf(dynamicOf(spec), nil, nameOption(spec.Name)...)
}

func dynamicOf(spec DynamicSpec) sup {
	b := sup{dynamic: true, max: intensity(spec.Intensity), period: spec.Period, maxChildren: spec.MaxChildren}
	if b.period <= 0 {
		b.period = DefaultPeriod
	}
	return b
}

// The requests of a dynamic supervisor, each with the type of its reply.
type (
	startChild struct {
		actor.Req[sup, startResult]
		spec ChildSpec
	}
	terminateChild struct {
		actor.Req[sup, error]
		pid proc.PID
	}
	countChildren struct{ actor.Req[sup, int] }
	startResult   struct {
		pid proc.PID
		err error
	}
)

// StartChild starts a child of the dynamic supervisor at sup and returns
// its PID. The ID of the spec is not used: children are known by PID, and
// a restarted child has a new one. A child whose Start returns
// actor.ErrIgnore is not kept, and StartChild returns actor.ErrIgnore.
func StartChild(ctx context.Context, caller actor.Caller, sup actor.Dest, spec ChildSpec) (proc.PID, error) {
	if spec.Start == nil {
		return proc.PID{}, errors.New("supervisor: child has no Start")
	}
	r, err := refTo(sup).Call(ctx, caller, startChild{spec: spec})
	if err != nil {
		return proc.PID{}, err
	}
	return r.pid, r.err
}

// TerminateChild stops the child pid of the dynamic supervisor at sup, as
// on shutdown, and forgets it.
func TerminateChild(ctx context.Context, caller actor.Caller, sup actor.Dest, pid proc.PID) error {
	notFound, err := refTo(sup).Call(ctx, caller, terminateChild{pid: pid})
	if err != nil {
		return err
	}
	return notFound
}

// CountChildren returns the number of children of the dynamic supervisor
// at sup, including those waiting to be restarted.
func CountChildren(ctx context.Context, caller actor.Caller, sup actor.Dest) (int, error) {
	return refTo(sup).Call(ctx, caller, countChildren{})
}
