package supervisor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Shutdown durations with a special meaning.
const (
	// Brutal kills the child without asking it to stop.
	Brutal time.Duration = -1
	// Infinity waits for the child to stop however long it takes.
	Infinity time.Duration = math.MaxInt64
)

// Default limits, as in OTP.
const (
	DefaultIntensity = 1
	DefaultPeriod    = 5 * time.Second
	DefaultShutdown  = 5 * time.Second
)

// ChildType tells workers from supervisors, which changes the default
// Shutdown.
type ChildType int

const (
	Worker ChildType = iota
	Supervisor
)

// Starter starts a child linked to parent and returns its PID, once it has
// started. It returns molecule.ErrIgnore for a child that is not to run,
// which the supervisor keeps without a process. genserver.Child,
// genstatem.Child, Child and DynamicChild make one, which gensim can
// simulate as well; StartFunc makes one of any function.
type Starter interface {
	StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error)
}

// StartFunc is a Starter of a function.
type StartFunc func(ctx context.Context, parent *proc.Self) (proc.PID, error)

func (f StartFunc) StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error) {
	return f(ctx, parent)
}

// ChildSpec describes a child.
type ChildSpec struct {
	ID      string
	Start   Starter
	Restart Restart
	// Shutdown is how long the child is given to stop after being asked
	// to with proc.Shutdown, before it is killed. Zero means
	// DefaultShutdown for a worker and Infinity for a supervisor.
	Shutdown time.Duration
	Type     ChildType
}

func (c ChildSpec) shutdown() time.Duration {
	switch {
	case c.Shutdown != 0:
		return c.Shutdown
	case c.Type == Supervisor:
		return Infinity
	}
	return DefaultShutdown
}

// Spec describes a supervisor.
type Spec struct {
	// Name, if set, is registered before the children start.
	Name     molecule.Name
	Strategy Strategy
	// If more than Intensity restarts happen within Period, the
	// supervisor terminates its children and exits with ErrMaxIntensity.
	// Zero values mean DefaultIntensity and DefaultPeriod.
	Intensity int
	Period    time.Duration
	// Children are started in order, and stopped in reverse order.
	Children []ChildSpec
}

// ErrMaxIntensity is the exit reason of a supervisor whose children
// restarted too often. It wraps proc.Shutdown.
var ErrMaxIntensity = fmt.Errorf("supervisor: reached maximum restart intensity: %w", proc.Shutdown)

// StartError is returned when a child fails to start along with its
// supervisor. The supervisor exits with it; it wraps both proc.Shutdown
// and the reason of the child.
type StartError struct {
	ID     string
	Reason error
}

func (e *StartError) Error() string {
	return fmt.Sprintf("supervisor: failed to start child %q: %v", e.ID, e.Reason)
}

func (e *StartError) Unwrap() []error { return []error{proc.Shutdown, e.Reason} }

// ChildInfo describes a child as WhichChildren reports it. PID is zero
// for a child that is not running.
type ChildInfo struct {
	ID      string
	PID     proc.PID
	Type    ChildType
	Restart Restart
	// Restarts counts the times a restart started the child again.
	Restarts int
}

// StartLink starts a supervisor linked to parent, and its children, and
// returns once they all have started. ctx bounds the whole start.
func StartLink(ctx context.Context, parent *proc.Self, spec Spec) (proc.PID, error) {
	if err := validate(spec); err != nil {
		return proc.PID{}, err
	}
	return gen.StartLink(ctx, parent, static(spec), nil, nameOption(spec.Name)...)
}

// Start starts a supervisor and its children, like StartLink but without
// a link, e.g. for the top supervisor started from main.
func Start(ctx context.Context, n *proc.Node, spec Spec) (proc.PID, error) {
	if err := validate(spec); err != nil {
		return proc.PID{}, err
	}
	return gen.Start(ctx, n, static(spec), nil, nameOption(spec.Name)...)
}

// Child returns the Starter of a supervisor, to nest it under another
// one, which gensim can simulate as well.
func Child(spec Spec) gen.Child {
	return gen.ChildOf(static(spec), nil, nameOption(spec.Name)...)
}

func static(spec Spec) sup {
	b := sup{strategy: spec.Strategy, max: spec.Intensity, period: spec.Period, specs: spec.Children}
	if b.max <= 0 {
		b.max = DefaultIntensity
	}
	if b.period <= 0 {
		b.period = DefaultPeriod
	}
	return b
}

func nameOption(name molecule.Name) []molecule.Option {
	if name == nil {
		return nil
	}
	return []molecule.Option{molecule.WithName(name)}
}

type (
	whichChildren struct{}
	stopReq       struct{}
)

// Stop stops the supervisor at sup, static or dynamic, as its parent
// exiting would: its children are stopped, then it exits with
// proc.Shutdown. It returns once the supervisor is dead.
func Stop(ctx context.Context, caller molecule.Caller, sup molecule.Dest) error {
	n := caller.Node()
	pid, ok := sup.WhereIs(n)
	if !ok {
		return &molecule.ExitError{To: sup, Reason: proc.NoProc}
	}
	down, release := n.Watch(ctx, pid)
	defer release()
	if _, err := molecule.Call(ctx, caller, pid, stopReq{}); err != nil {
		return err
	}
	<-down.Done()
	return ctx.Err()
}

// WhichChildren returns the children of the supervisor at sup, in start
// order.
func WhichChildren(ctx context.Context, caller molecule.Caller, sup molecule.Dest) ([]ChildInfo, error) {
	v, err := molecule.Call(ctx, caller, sup, whichChildren{})
	if err != nil {
		return nil, err
	}
	return v.([]ChildInfo), nil
}

func validate(spec Spec) error {
	ids := make(map[string]bool)
	for _, c := range spec.Children {
		switch {
		case c.ID == "":
			return errors.New("supervisor: child without an ID")
		case ids[c.ID]:
			return fmt.Errorf("supervisor: duplicate child ID %q", c.ID)
		case c.Start == nil:
			return fmt.Errorf("supervisor: child %q has no Start", c.ID)
		}
		ids[c.ID] = true
	}
	switch spec.Strategy {
	case OneForOne, OneForAll, RestForOne:
	default:
		return fmt.Errorf("supervisor: unknown strategy %d", spec.Strategy)
	}
	return nil
}
