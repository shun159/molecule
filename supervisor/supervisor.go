// Package supervisor starts, watches and restarts child processes, like
// Erlang's supervisor.
//
// Unlike gen behaviours, a supervisor runs no user callbacks: what it does
// is given as data, a Spec. Its decisions (whether to restart, whether the
// restart intensity is exceeded, which children to stop and start) are
// pure functions; this package runs them against the processes.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/shun159/molecule/gen"
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

// StartFunc starts a child linked to parent and returns its PID. It
// returns gen.ErrIgnore for a child that is not to run, which the
// supervisor keeps without a process. genserver.StartLinkFunc and
// StartLinkFunc make one.
type StartFunc func(ctx context.Context, parent *proc.Self) (proc.PID, error)

// ChildSpec describes a child.
type ChildSpec struct {
	ID      string
	Start   StartFunc
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
	Name     gen.Name
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
}

// StartLink starts a supervisor linked to parent, and its children, and
// returns once they all have started. ctx bounds the whole start.
func StartLink(ctx context.Context, parent *proc.Self, spec Spec) (proc.PID, error) {
	return parent.StartLink(ctx, func(s *proc.Self) error { return run(ctx, s, spec) })
}

// Start starts a supervisor and its children, like StartLink but without
// a link, e.g. for the top supervisor started from main.
func Start(ctx context.Context, n *proc.Node, spec Spec) (proc.PID, error) {
	return n.Start(ctx, func(s *proc.Self) error { return run(ctx, s, spec) })
}

// StartLinkFunc returns a StartFunc that starts a supervisor, to nest it
// under another one.
func StartLinkFunc(spec Spec) StartFunc {
	return func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return StartLink(ctx, parent, spec)
	}
}

type (
	whichChildren struct{}
	stopReq       struct{}
)

// Stop stops the supervisor at sup, static or dynamic, as its parent
// exiting would: its children are stopped, then it exits with
// proc.Shutdown. It returns once the supervisor is dead.
func Stop(ctx context.Context, caller gen.Caller, sup gen.Dest) error {
	n := caller.Node()
	pid, ok := sup.WhereIs(n)
	if !ok {
		return &gen.ExitError{To: sup, Reason: proc.NoProc}
	}
	down, release := n.Watch(ctx, pid)
	defer release()
	if _, err := gen.Call(ctx, caller, pid, stopReq{}); err != nil {
		return err
	}
	<-down.Done()
	return ctx.Err()
}

// WhichChildren returns the children of the supervisor at sup, in start
// order.
func WhichChildren(ctx context.Context, caller gen.Caller, sup gen.Dest) ([]ChildInfo, error) {
	v, err := gen.Call(ctx, caller, sup, whichChildren{})
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

type child struct {
	spec ChildSpec
	pid  proc.PID
}

type supervisor struct {
	self     *proc.Self
	strategy Strategy
	children []*child
	restarts intensity
}

// retry asks the supervisor to try again to restart a child that failed
// to start during a restart, as OTP does.
type retry struct{ id string }

func run(ctx context.Context, self *proc.Self, spec Spec) error {
	self.TrapExit(true)
	if err := validate(spec); err != nil {
		self.InitAck(err)
		return nil
	}
	if spec.Name != nil {
		if err := spec.Name.Register(self.Node(), self.PID()); err != nil {
			if pid, ok := spec.Name.WhereIs(self.Node()); ok && pid != self.PID() {
				err = &gen.AlreadyStartedError{PID: pid}
			}
			self.InitAck(err)
			return nil
		}
	}

	s := &supervisor{
		self:     self,
		strategy: spec.Strategy,
		restarts: intensity{max: spec.Intensity, period: spec.Period},
	}
	if s.restarts.max <= 0 {
		s.restarts.max = DefaultIntensity
	}
	if s.restarts.period <= 0 {
		s.restarts.period = DefaultPeriod
	}

	for _, cs := range spec.Children {
		c := &child{spec: cs}
		if err := s.start(ctx, c); err != nil {
			reportStartFailed(self, cs.ID, err)
			s.terminateAll()
			err = &StartError{ID: cs.ID, Reason: err}
			self.InitAck(err)
			return err
		}
		s.children = append(s.children, c)
	}
	self.InitAck(nil)
	return s.loop()
}

func (s *supervisor) loop() error {
	for {
		msg, err := s.self.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case proc.ExitMsg:
			if parent := s.self.Parent(); !parent.IsZero() && m.From == parent {
				s.terminateAll()
				return m.Reason
			}
			c := s.byPID(m.From)
			if c == nil {
				continue // a child already stopped or replaced
			}
			reportTerminated(s.self, c.spec.ID, m.From, c.spec.Restart, m.Reason)
			c.pid = proc.PID{}
			if err := s.exited(c, m.Reason); err != nil {
				reportShutdown(s.self, err)
				s.terminateAll()
				return err
			}
		case retry:
			c := s.byID(m.id)
			if c == nil || !c.pid.IsZero() {
				continue
			}
			if err := s.restart(c); err != nil {
				reportShutdown(s.self, err)
				s.terminateAll()
				return err
			}
		case gen.CallMsg:
			switch m.Req.(type) {
			case whichChildren:
				gen.SendReply(s.self, m.From, s.which())
			case stopReq:
				s.terminateAll()
				gen.SendReply(s.self, m.From, nil)
				return proc.Shutdown
			}
		}
	}
}

// exited handles the exit of child c with reason.
func (s *supervisor) exited(c *child, reason error) error {
	if !shouldRestart(c.spec.Restart, reason) {
		if c.spec.Restart == Temporary {
			s.remove(c)
		}
		return nil
	}
	return s.restart(c)
}

// restart restarts c, which is not running, and the children the strategy
// involves.
func (s *supervisor) restart(c *child) error {
	var ok bool
	if s.restarts, ok = s.restarts.add(time.Now()); !ok {
		return ErrMaxIntensity
	}
	stop, start := plan(s.strategy, s.index(c), len(s.children))
	for _, i := range stop {
		s.shutdown(s.children[i])
	}
	for _, i := range start {
		cc := s.children[i]
		if !cc.pid.IsZero() {
			continue
		}
		if err := s.start(context.Background(), cc); err != nil {
			reportStartFailed(s.self, cc.spec.ID, err)
			// The children after it stay down until the retry.
			s.self.Send(s.self.PID(), retry{id: cc.spec.ID})
			return nil
		}
	}
	return nil
}

func (s *supervisor) start(ctx context.Context, c *child) error {
	pid, err := c.spec.Start(ctx, s.self)
	switch {
	case errors.Is(err, gen.ErrIgnore):
		c.pid = proc.PID{}
		return nil
	case err != nil:
		return err
	}
	c.pid = pid
	reportStarted(s.self, c.spec.ID, pid)
	return nil
}

// shutdown stops c and waits until it is dead. See stopAll.
func (s *supervisor) shutdown(c *child) {
	stopAll(s.self, []*child{c})
}

// terminateAll stops the children in reverse start order.
func (s *supervisor) terminateAll() {
	for i := len(s.children) - 1; i >= 0; i-- {
		s.shutdown(s.children[i])
	}
}

func (s *supervisor) which() []ChildInfo {
	infos := make([]ChildInfo, len(s.children))
	for i, c := range s.children {
		infos[i] = ChildInfo{ID: c.spec.ID, PID: c.pid, Type: c.spec.Type, Restart: c.spec.Restart}
	}
	return infos
}

func (s *supervisor) index(c *child) int {
	for i, cc := range s.children {
		if cc == c {
			return i
		}
	}
	return -1
}

func (s *supervisor) byPID(pid proc.PID) *child {
	if pid.IsZero() {
		return nil // children not running have a zero PID
	}
	for _, c := range s.children {
		if c.pid == pid {
			return c
		}
	}
	return nil
}

func (s *supervisor) byID(id string) *child {
	for _, c := range s.children {
		if c.spec.ID == id {
			return c
		}
	}
	return nil
}

func (s *supervisor) remove(c *child) {
	if i := s.index(c); i >= 0 {
		s.children = append(s.children[:i:i], s.children[i+1:]...)
	}
}
