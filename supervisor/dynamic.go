package supervisor

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// DynamicSpec describes a dynamic supervisor, which starts with no
// children and starts them on demand with StartChild, like Elixir's
// DynamicSupervisor or OTP's simple_one_for_one. Its children are
// independent: each is restarted on its own, as with OneForOne, and they
// are all stopped at once rather than in order.
type DynamicSpec struct {
	// Name, if set, is registered when the supervisor starts.
	Name gen.Name
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
	return parent.StartLink(ctx, func(s *proc.Self) error { return runDynamic(s, spec) })
}

// StartDynamic starts a dynamic supervisor, like StartDynamicLink but
// without a link.
func StartDynamic(ctx context.Context, n *proc.Node, spec DynamicSpec) (proc.PID, error) {
	return n.Start(ctx, func(s *proc.Self) error { return runDynamic(s, spec) })
}

// StartDynamicLinkFunc returns a StartFunc that starts a dynamic
// supervisor, to nest it under another supervisor.
func StartDynamicLinkFunc(spec DynamicSpec) StartFunc {
	return func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return StartDynamicLink(ctx, parent, spec)
	}
}

type (
	startChild     struct{ spec ChildSpec }
	terminateChild struct{ pid proc.PID }
	countChildren  struct{}
	startResult    struct {
		pid proc.PID
		err error
	}
)

// StartChild starts a child of the dynamic supervisor at sup and returns
// its PID. The ID of the spec is not used: children are known by PID, and
// a restarted child has a new one. A child whose Start returns
// gen.ErrIgnore is not kept, and StartChild returns gen.ErrIgnore.
func StartChild(ctx context.Context, caller gen.Caller, sup gen.Dest, spec ChildSpec) (proc.PID, error) {
	if spec.Start == nil {
		return proc.PID{}, errors.New("supervisor: child has no Start")
	}
	v, err := gen.Call(ctx, caller, sup, startChild{spec})
	if err != nil {
		return proc.PID{}, err
	}
	r := v.(startResult)
	return r.pid, r.err
}

// TerminateChild stops the child pid of the dynamic supervisor at sup, as
// on shutdown, and forgets it.
func TerminateChild(ctx context.Context, caller gen.Caller, sup gen.Dest, pid proc.PID) error {
	v, err := gen.Call(ctx, caller, sup, terminateChild{pid})
	if err != nil {
		return err
	}
	err, _ = v.(error)
	return err
}

// CountChildren returns the number of children of the dynamic supervisor
// at sup, including those waiting to be restarted.
func CountChildren(ctx context.Context, caller gen.Caller, sup gen.Dest) (int, error) {
	v, err := gen.Call(ctx, caller, sup, countChildren{})
	if err != nil {
		return 0, err
	}
	return v.(int), nil
}

type dynamicChild struct {
	child
	seq uint64 // start order, to list children stably
}

type dynamic struct {
	self     *proc.Self
	max      int
	restarts intensity
	seq      uint64
	running  map[proc.PID]*dynamicChild
	retrying map[uint64]*dynamicChild // by seq
}

// dynamicRetry asks to try again to restart a child, as retry does.
type dynamicRetry struct{ seq uint64 }

func runDynamic(self *proc.Self, spec DynamicSpec) error {
	self.TrapExit(true)
	if spec.Name != nil {
		if err := spec.Name.Register(self.Node(), self.PID()); err != nil {
			if pid, ok := spec.Name.WhereIs(self.Node()); ok && pid != self.PID() {
				err = &gen.AlreadyStartedError{PID: pid}
			}
			self.InitAck(err)
			return nil
		}
	}
	d := &dynamic{
		self:     self,
		max:      spec.MaxChildren,
		restarts: intensity{max: spec.Intensity, period: spec.Period},
		running:  make(map[proc.PID]*dynamicChild),
		retrying: make(map[uint64]*dynamicChild),
	}
	if d.restarts.max <= 0 {
		d.restarts.max = DefaultIntensity
	}
	if d.restarts.period <= 0 {
		d.restarts.period = DefaultPeriod
	}
	self.InitAck(nil)
	return d.loop()
}

func (d *dynamic) loop() error {
	for {
		msg, err := d.self.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case proc.ExitMsg:
			if parent := d.self.Parent(); !parent.IsZero() && m.From == parent {
				d.terminateAll()
				return m.Reason
			}
			c, ok := d.running[m.From]
			if !ok {
				continue
			}
			delete(d.running, m.From)
			if !shouldRestart(c.spec.Restart, m.Reason) {
				continue // forgotten: a child is only known by its PID
			}
			if err := d.restart(c); err != nil {
				d.terminateAll()
				return err
			}
		case dynamicRetry:
			c, ok := d.retrying[m.seq]
			if !ok {
				continue
			}
			delete(d.retrying, m.seq)
			if err := d.restart(c); err != nil {
				d.terminateAll()
				return err
			}
		case gen.CallMsg:
			gen.SendReply(d.self, m.From, d.call(m.Req))
		}
	}
}

func (d *dynamic) call(req any) any {
	switch r := req.(type) {
	case startChild:
		if d.max > 0 && d.count() >= d.max {
			return startResult{err: ErrMaxChildren}
		}
		d.seq++
		c := &dynamicChild{child: child{spec: r.spec}, seq: d.seq}
		if err := d.start(c); err != nil {
			return startResult{err: err}
		}
		return startResult{pid: c.pid}
	case terminateChild:
		c, ok := d.running[r.pid]
		if !ok {
			return ErrNotFound
		}
		delete(d.running, r.pid)
		stopAll(d.self, []*child{&c.child})
		return nil
	case countChildren:
		return d.count()
	case whichChildren:
		return d.which()
	}
	return nil
}

// start starts c and records it as running. A child that is ignored is
// not recorded, and gen.ErrIgnore is returned.
func (d *dynamic) start(c *dynamicChild) error {
	pid, err := c.spec.Start(context.Background(), d.self)
	if err != nil {
		return err
	}
	c.pid = pid
	d.running[pid] = c
	return nil
}

func (d *dynamic) restart(c *dynamicChild) error {
	var ok bool
	if d.restarts, ok = d.restarts.add(time.Now()); !ok {
		return ErrMaxIntensity
	}
	err := d.start(c)
	if err != nil && !errors.Is(err, gen.ErrIgnore) {
		d.retrying[c.seq] = c
		d.self.Send(d.self.PID(), dynamicRetry{seq: c.seq})
	}
	return nil
}

func (d *dynamic) count() int { return len(d.running) + len(d.retrying) }

func (d *dynamic) sorted() []*dynamicChild {
	return slices.SortedFunc(maps.Values(d.running), func(a, b *dynamicChild) int {
		return cmp.Compare(a.seq, b.seq)
	})
}

func (d *dynamic) which() []ChildInfo {
	var infos []ChildInfo
	for _, c := range d.sorted() {
		infos = append(infos, ChildInfo{PID: c.pid, Type: c.spec.Type, Restart: c.spec.Restart})
	}
	return infos
}

// terminateAll stops all the children at once.
func (d *dynamic) terminateAll() {
	var cs []*child
	for _, c := range d.sorted() {
		cs = append(cs, &c.child)
	}
	stopAll(d.self, cs)
	clear(d.running)
	clear(d.retrying)
}
