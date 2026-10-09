package gensim

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// DefaultCallTimeout bounds the virtual time a Call waits, as
// gen_server:call does by default.
const DefaultCallTimeout = 5 * time.Second

// Sim is a simulated node running behaviours in one goroutine, in an order
// decided by its seed, on a virtual clock.
type Sim struct {
	rng    *rand.Rand
	logger *slog.Logger
	now    time.Time

	nodes        map[string]*node
	nodeWatchers map[proc.PID]bool  // the processes monitoring nodes
	def          *node              // where processes run unless told otherwise
	cuts         map[[2]string]bool // pairs of nodes that cannot reach each other

	procs []*process // in creation order
	byPID map[proc.PID]*process

	links  []*link // in creation order, possibly empty
	byLink map[[2]proc.PID]*link

	aliases map[proc.Ref]*alias
	timers  timerHeap
	seq     uint64

	trace []Event

	noPurity bool

	// CallTimeout bounds the virtual time a Call waits.
	CallTimeout time.Duration
	// Loss is the probability that a message between two nodes is lost,
	// beyond the losses of partitions and crashes. Erlang loses none
	// while connected; a protocol that must bear it, as Raft, is tested
	// with it.
	Loss float64
}

// DefaultNode is the node of the processes spawned without On.
const DefaultNode = "sim"

// node is a simulated node.
type node struct {
	name  string
	alloc *proc.Node // allocates its PIDs and references
	names map[string]proc.PID
	up    bool
	// creation tells its incarnations apart, as for a proc.Node.
	creation uint32
}

// process is a simulated process.
type process struct {
	node        *node
	on          string // the node to spawn on
	pid, parent proc.PID
	runner      gen.Runner
	mailbox     []any
	alive       bool
	busy        bool // handling, or starting: Step leaves it alone
	acked       bool // its start is over
	ackErr      error
	exitReason  error
	name        string
	trap        bool
	linked      map[proc.PID]bool
	watchers    map[proc.Ref]watcher // who monitors it
	watching    map[proc.Ref]proc.PID
}

// watcher is a process monitoring another, or an alias that does.
type watcher struct {
	pid   proc.PID
	alias bool
}

// link carries the messages from one process to another, in order.
type link struct {
	from, to proc.PID
	queue    []flight
}

// flight is a message in flight, and its fingerprint when sent.
type flight struct {
	msg any
	fp  string
}

// alias is an alias made by Request or Call.
type alias struct {
	owner  proc.PID // zero: the driver
	target proc.PID // monitored, if any
	reply  func(proc.Ref, proc.AliasMsg) any
	done   bool
	result proc.AliasMsg
}

// Option configures a Sim.
type Option func(*Sim)

// WithLogger sends the reports of the simulated behaviours to l. They are
// discarded otherwise.
func WithLogger(l *slog.Logger) Option { return func(s *Sim) { s.logger = l } }

// New returns a simulation whose order of events is decided by seed: the
// same seed, the same behaviours and the same calls make the same run.
func New(seed uint64, opts ...Option) *Sim {
	s := &Sim{
		rng:          rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		logger:       slog.New(slog.DiscardHandler),
		now:          time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		nodes:        make(map[string]*node),
		nodeWatchers: make(map[proc.PID]bool),
		cuts:         make(map[[2]string]bool),
		byPID:        make(map[proc.PID]*process),
		byLink:       make(map[[2]proc.PID]*link),
		aliases:      make(map[proc.Ref]*alias),
		CallTimeout:  DefaultCallTimeout,
	}
	s.def = s.nodeNamed(DefaultNode)
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// nodeNamed returns the node name, made on first use.
func (s *Sim) nodeNamed(name string) *node {
	n := s.nodes[name]
	if n == nil {
		n = &node{name: name, alloc: proc.NewNode(name, proc.WithCreation(1)), names: make(map[string]proc.PID), up: true, creation: 1}
		s.nodes[name] = n
		// A node is connected to the others once it exists.
		for _, other := range s.nodeNames() {
			if other != name && s.connected(name, other) {
				s.nodeEvent(name, other, true)
			}
		}
	}
	return n
}

// Now returns the virtual time.
func (s *Sim) Now() time.Time { return s.now }

// SpawnOption configures Spawn.
type SpawnOption func(*process)

// Named registers the process under name, as a molecule.Local name of its
// node.
func Named(name string) SpawnOption { return func(p *process) { p.name = name } }

// On spawns the process on the node name, made on first use, rather than
// on DefaultNode.
func On(name string) SpawnOption { return func(p *process) { p.on = name } }

// ErrNodeDown is the error of spawning on a node crashed.
var ErrNodeDown = errors.New("gensim: node down")

// Spawn starts b in a new simulated process and runs its Init, as gen.Start
// does. An error from Init is returned, the process exiting with it.
func Spawn[S any](s *Sim, b gen.Behaviour[S], args any, opts ...SpawnOption) (proc.PID, error) {
	p := &process{}
	for _, opt := range opts {
		opt(p)
	}
	node := s.def
	if p.on != "" {
		node = s.nodeNamed(p.on)
	}
	return s.start(node, nil, fmt.Sprintf("%T", b), func(e gen.Env) (gen.Runner, any, string) {
		return gen.NewRunner(b, e), args, p.name
	})
}

// Start starts c, a child as genserver.Child or supervisor.Child make it,
// on its own, as Spawn does: a whole supervision tree, from its top.
func Start(s *Sim, c gen.Child, opts ...SpawnOption) (proc.PID, error) {
	p := &process{}
	for _, opt := range opts {
		opt(p)
	}
	node := s.def
	if p.on != "" {
		node = s.nodeNamed(p.on)
	}
	return s.start(node, nil, fmt.Sprintf("%T", c), func(e gen.Env) (gen.Runner, any, string) {
		r, args, o := c.Simulate(e)
		name := p.name
		if l, ok := o.Name.(molecule.Local); ok && name == "" {
			name = string(l)
		}
		return r, args, name
	})
}

// ErrNotSimulated is the error of starting a child a simulation cannot
// run: one started by a function, rather than a behaviour.
var ErrNotSimulated = errors.New("gensim: child started by a function, which cannot be simulated")

// start starts a process on node, linked to parent if any, running the
// runner newRunner makes, registered under the name it tells, if any, and
// runs the simulation until it has started, as Start and StartLink wait.
func (s *Sim) start(node *node, parent *process, what string, newRunner func(gen.Env) (gen.Runner, any, string)) (proc.PID, error) {
	if !node.up {
		return proc.PID{}, ErrNodeDown
	}
	p := &process{
		node:     node,
		pid:      node.alloc.NewPID(),
		alive:    true,
		linked:   make(map[proc.PID]bool),
		watchers: make(map[proc.Ref]watcher),
		watching: make(map[proc.Ref]proc.PID),
	}
	runner, args, name := newRunner(&env{s, p})
	if name != "" {
		if pid, ok := node.names[name]; ok {
			return proc.PID{}, &molecule.AlreadyStartedError{PID: pid}
		}
		node.names[name] = p.pid
		p.name = name
	}
	if parent != nil {
		p.parent = parent.pid
		p.linked[parent.pid] = true
		parent.linked[p.pid] = true
	}
	s.procs = append(s.procs, p)
	s.byPID[p.pid] = p
	s.record(Event{Kind: Spawned, To: p.pid, Msg: what})

	p.runner = runner
	p.busy = true
	err := runner.Init(args)
	if err != nil {
		p.busy = false
		reason := err
		if errors.Is(err, molecule.ErrIgnore) {
			reason = proc.Normal
		}
		s.exit(p, reason)
		return proc.PID{}, err
	}
	done, reason := runner.Flush()
	p.busy = false
	if done {
		s.exit(p, reason)
	}
	// A start held back runs on, until it is told over, or the process
	// dies.
	for p.alive && !p.acked {
		if !s.Step() && !s.fireTimer(s.now.Add(s.CallTimeout)) {
			break
		}
	}
	switch {
	case p.ackErr != nil:
		return proc.PID{}, p.ackErr
	case !p.acked && !p.alive:
		return proc.PID{}, p.exitReason
	case !p.acked:
		return proc.PID{}, errors.New("gensim: start never acknowledged")
	}
	return p.pid, nil
}

// Send sends msg to the process at to, from outside the simulation.
func (s *Sim) Send(to molecule.Dest, msg any) {
	if pid, ok := s.resolve(to); ok {
		s.send(proc.PID{}, pid, msg)
	}
}

// Cast casts req to the server at to, as molecule.SendCast does.
func (s *Sim) Cast(to molecule.Dest, req any) { s.Send(to, molecule.CastMsg{Req: req}) }

// Call calls the server at to and runs the simulation until the reply
// arrives, as molecule.Call does. Time passes, timers firing, while it waits,
// up to CallTimeout, after which Call returns context.DeadlineExceeded, the
// server told with a molecule.CallAbandoned, as molecule.Call does.
func (s *Sim) Call(to molecule.Dest, req any) (any, error) {
	pid, ok := s.resolve(to)
	if !ok {
		return nil, &molecule.ExitError{To: to, Reason: proc.NoProc}
	}
	ref, a := s.newAlias(proc.PID{}, pid, nil)
	s.send(proc.PID{}, pid, molecule.CallMsg{From: molecule.From{Tag: ref}, Req: req})

	deadline := s.now.Add(s.CallTimeout)
	for !a.done {
		if s.Step() {
			continue
		}
		if !s.fireTimer(deadline) {
			s.releaseAlias(ref)
			s.now = deadline
			s.send(proc.PID{}, pid, molecule.CallAbandoned{From: molecule.From{Tag: ref}})
			return nil, context.DeadlineExceeded
		}
	}
	if a.result.Down {
		return nil, &molecule.ExitError{To: to, Reason: a.result.Reason}
	}
	return a.result.Msg, nil
}

// Exit sends an exit signal with reason to the process pid from outside
// the simulation, as proc.Self.Exit does: proc.Kill kills it.
func (s *Sim) Exit(pid proc.PID, reason error) {
	if p := s.byPID[pid]; p != nil && p.alive {
		s.signal(p, proc.PID{}, reason, false)
	}
}

// Alive reports whether the process pid is alive.
func (s *Sim) Alive(pid proc.PID) bool {
	p := s.byPID[pid]
	return p != nil && p.alive
}

// WhereIs returns the process registered under name on DefaultNode, or
// on another node with a name of the form molecule.Remote takes, see Resolve.
func (s *Sim) WhereIs(name string) (proc.PID, bool) {
	pid, ok := s.def.names[name]
	return pid, ok
}

// Resolve returns the process at dest, as the driver sees it: a
// molecule.Local name is of DefaultNode, and a molecule.Remote name of any node.
func (s *Sim) Resolve(dest molecule.Dest) (proc.PID, bool) { return s.resolve(dest) }

// State returns the state of the behaviour of pid.
func State[S any](s *Sim, pid proc.PID) (S, bool) {
	var zero S
	p := s.byPID[pid]
	if p == nil || !p.alive {
		return zero, false
	}
	st, ok := p.runner.State().(S)
	return st, ok
}

// Step takes one step of the simulation: it delivers a message in flight
// to a mailbox, or has a process handle a message, the choice among all
// possible ones decided by the seed. It reports false when no step is
// possible, until time passes.
func (s *Sim) Step() bool {
	var choices []func()
	for _, l := range s.links {
		if len(l.queue) > 0 {
			choices = append(choices, func() { s.deliver(l) })
		}
	}
	for _, p := range s.procs {
		// A process busy starting another handles nothing meanwhile.
		if p.alive && !p.busy && len(p.mailbox) > 0 {
			choices = append(choices, func() { s.handle(p) })
		}
	}
	if len(choices) == 0 {
		return false
	}
	choices[s.rng.IntN(len(choices))]()
	return true
}

// RunUntilIdle takes steps until none is possible without time passing.
func (s *Sim) RunUntilIdle() {
	for s.Step() {
	}
}

// Advance runs the simulation for d of virtual time: whenever nothing else
// can happen, time moves to the next timer, which fires.
func (s *Sim) Advance(d time.Duration) {
	end := s.now.Add(d)
	s.RunUntilIdle()
	for s.fireTimer(end) {
		s.RunUntilIdle()
	}
	s.now = end
}

// Run runs the simulation for d of virtual time, as Advance does, and
// calls check after every step and every timer: an invariant of the
// system, checked at every point. It stops at the first error check
// returns, and returns it.
func (s *Sim) Run(d time.Duration, check func() error) error {
	end := s.now.Add(d)
	for {
		for s.Step() {
			if err := check(); err != nil {
				return err
			}
		}
		if !s.fireTimer(end) {
			break
		}
		if err := check(); err != nil {
			return err
		}
	}
	s.now = end
	return nil
}

func (s *Sim) resolve(dest molecule.Dest) (proc.PID, bool) {
	switch d := dest.(type) {
	case proc.PID:
		return d, !d.IsZero()
	case molecule.Local:
		pid, ok := s.def.names[string(d)]
		return pid, ok
	case molecule.Remote:
		if n := s.nodes[d.Node]; n != nil {
			pid, ok := n.names[d.Name]
			return pid, ok
		}
		return proc.PID{}, false
	}
	return dest.WhereIs(s.def.alloc)
}

// reachable reports whether what from sends reaches to: the driver, whose
// PID is zero, reaches all nodes up.
func (s *Sim) reachable(from, to proc.PID) bool {
	tn := s.nodes[to.Node()]
	if tn == nil || !tn.up {
		return false
	}
	if from.IsZero() || from.Node() == to.Node() {
		return true
	}
	fn := s.nodes[from.Node()]
	return fn != nil && fn.up && !s.cuts[[2]string{from.Node(), to.Node()}]
}

// send puts msg in flight from one process to another, unless the
// network loses it.
func (s *Sim) send(from, to proc.PID, msg any) {
	s.record(Event{Kind: Sent, From: from, To: to, Msg: msg})
	if !s.reachable(from, to) {
		s.record(Event{Kind: Dropped, From: from, To: to, Msg: msg})
		return
	}
	if s.Loss > 0 && !from.IsZero() && from.Node() != to.Node() && s.rng.Float64() < s.Loss {
		s.record(Event{Kind: Dropped, From: from, To: to, Msg: msg})
		return
	}
	key := [2]proc.PID{from, to}
	l := s.byLink[key]
	if l == nil {
		l = &link{from: from, to: to}
		s.byLink[key] = l
		s.links = append(s.links, l)
	}
	f := flight{msg: msg}
	if !s.noPurity {
		f.fp = fingerprint(msg)
	}
	l.queue = append(l.queue, f)
}

// deliver moves the first message in flight on l to its mailbox.
func (s *Sim) deliver(l *link) {
	f := l.queue[0]
	l.queue[0] = flight{}
	l.queue = l.queue[1:]
	msg := f.msg
	if !s.noPurity && fingerprint(msg) != f.fp {
		panic(&ImpureError{PID: l.from, Msg: msg, What: "changed a message after sending it to " + l.to.String()})
	}
	if p := s.byPID[l.to]; p != nil && p.alive {
		p.mailbox = append(p.mailbox, msg)
	}
}

// handle has p handle its first message.
func (s *Sim) handle(p *process) {
	msg := p.mailbox[0]
	p.mailbox[0] = nil
	p.mailbox = p.mailbox[1:]
	s.record(Event{Kind: Handled, To: p.pid, Msg: msg})
	var old any
	var before string
	if !s.noPurity {
		old = p.runner.State()
		before = fingerprint(old)
	}
	p.busy = true
	done, reason := p.runner.Deliver(msg)
	p.busy = false
	if !s.noPurity && fingerprint(old) != before {
		panic(&ImpureError{PID: p.pid, Msg: msg, What: "changed the state it was given, handling a message"})
	}
	if done {
		s.exit(p, reason)
	}
}

// exit makes p dead with reason and tells its links and monitors.
func (s *Sim) exit(p *process, reason error) {
	if !p.alive {
		return
	}
	p.alive = false
	p.exitReason = reason
	p.runner.Abort()
	p.mailbox = nil
	s.record(Event{Kind: Exited, To: p.pid, Msg: reason})
	if p.name != "" && p.node.names[p.name] == p.pid {
		delete(p.node.names, p.name)
	}
	for ref, target := range p.watching {
		if t := s.byPID[target]; t != nil {
			delete(t.watchers, ref)
		}
	}
	for _, pid := range sortedPIDs(p.linked) {
		delete(p.linked, pid)
		if q := s.byPID[pid]; q != nil && q.alive {
			delete(q.linked, p.pid)
			s.signal(q, p.pid, reason, true)
		}
	}
	refs := make([]proc.Ref, 0, len(p.watchers))
	for ref := range p.watchers {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b proc.Ref) int { return cmp.Compare(a.String(), b.String()) })
	for _, ref := range refs {
		w := p.watchers[ref]
		delete(p.watchers, ref)
		if w.alias {
			s.aliasDown(ref, p.pid, reason)
			continue
		}
		if q := s.byPID[w.pid]; q != nil && q.alive {
			delete(q.watching, ref)
			s.send(p.pid, w.pid, proc.DownMsg{Ref: ref, PID: p.pid, Reason: reason})
		}
	}
}

// signal delivers an exit signal from from to p, as proc does.
func (s *Sim) signal(p *process, from proc.PID, reason error, viaLink bool) {
	switch {
	case !viaLink && errors.Is(reason, proc.Kill):
		s.exit(p, proc.Killed)
	case p.trap:
		s.send(from, p.pid, proc.ExitMsg{From: from, Reason: reason})
	case errors.Is(reason, proc.Normal):
	default:
		s.exit(p, reason)
	}
}

func (s *Sim) newAlias(owner, target proc.PID, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, *alias) {
	alloc := s.def.alloc
	if p := s.byPID[owner]; p != nil {
		alloc = p.node.alloc
	}
	ref := alloc.MakeRef()
	a := &alias{owner: owner, target: target, reply: reply}
	s.aliases[ref] = a
	t := s.byPID[target]
	switch {
	case !s.reachable(owner, target):
		s.answer(ref, owner, proc.AliasMsg{Down: true, Reason: proc.NoConnection})
	case t != nil && t.alive:
		t.watchers[ref] = watcher{alias: true}
	default:
		s.aliasDown(ref, target, proc.NoProc)
	}
	return ref, a
}

// answer delivers the first message to an alias.
func (s *Sim) answer(ref proc.Ref, from proc.PID, m proc.AliasMsg) {
	a := s.aliases[ref]
	if a == nil {
		return
	}
	s.releaseAlias(ref)
	a.done, a.result = true, m
	if a.reply != nil {
		s.send(from, a.owner, a.reply(ref, m))
	}
}

func (s *Sim) aliasDown(ref proc.Ref, target proc.PID, reason error) {
	s.answer(ref, target, proc.AliasMsg{Down: true, Reason: reason})
}

func (s *Sim) releaseAlias(ref proc.Ref) {
	a := s.aliases[ref]
	if a == nil {
		return
	}
	delete(s.aliases, ref)
	if t := s.byPID[a.target]; t != nil {
		delete(t.watchers, ref)
	}
}

// fireTimer moves time to the next timer, if not past limit, and fires it.
func (s *Sim) fireTimer(limit time.Time) bool {
	for len(s.timers) > 0 && s.timers[0].cancelled {
		heap.Pop(&s.timers)
	}
	if len(s.timers) == 0 || s.timers[0].at.After(limit) {
		return false
	}
	t := heap.Pop(&s.timers).(*simTimer)
	if t.at.After(s.now) {
		s.now = t.at
	}
	if p := s.byPID[t.pid]; p != nil && p.alive {
		s.record(Event{Kind: Fired, To: t.pid, Msg: t.msg})
		p.mailbox = append(p.mailbox, t.msg)
	}
	return true
}

func (s *Sim) record(e Event) {
	e.At = s.now
	s.trace = append(s.trace, e)
}

func sortedPIDs(set map[proc.PID]bool) []proc.PID {
	pids := make([]proc.PID, 0, len(set))
	for pid := range set {
		pids = append(pids, pid)
	}
	slices.SortFunc(pids, func(a, b proc.PID) int { return cmp.Compare(a.String(), b.String()) })
	return pids
}

// simTimer is a message to deliver at a virtual time.
type simTimer struct {
	at        time.Time
	seq       uint64
	pid       proc.PID
	msg       any
	cancelled bool
}

type timerHeap []*simTimer

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(*simTimer)) }
func (h *timerHeap) Pop() any {
	old := *h
	t := old[len(old)-1]
	*h = old[:len(old)-1]
	return t
}

// env is the gen.Env of a simulated process.
type env struct {
	s *Sim
	p *process
}

func (e *env) Self() proc.PID   { return e.p.pid }
func (e *env) Parent() proc.PID { return e.p.parent }

// Resolve resolves dest as on the node of the process: a molecule.Remote name
// of another node does not resolve, as in proc.
func (e *env) Resolve(dest molecule.Dest) (proc.PID, bool) {
	switch d := dest.(type) {
	case molecule.Local:
		pid, ok := e.p.node.names[string(d)]
		return pid, ok
	case molecule.Remote:
		if d.Node != e.p.node.name {
			return proc.PID{}, false
		}
		pid, ok := e.p.node.names[d.Name]
		return pid, ok
	case proc.PID:
		return d, !d.IsZero()
	}
	return dest.WhereIs(e.p.node.alloc)
}

func (e *env) Send(to proc.PID, msg any) { e.s.send(e.p.pid, to, msg) }

// SendName sends to a name of a simulated node, resolved at once.
func (e *env) SendName(node, name string, msg any) {
	if pid, ok := e.s.resolve(molecule.Remote{Node: node, Name: name}); ok {
		e.s.send(e.p.pid, pid, msg)
	}
}

func (e *env) SendAlias(ref proc.Ref, msg any) {
	e.s.answer(ref, e.p.pid, proc.AliasMsg{Msg: msg})
}

func (e *env) Monitor(pid proc.PID) proc.Ref {
	ref := e.p.node.alloc.MakeRef()
	t := e.s.byPID[pid]
	if !e.s.reachable(e.p.pid, pid) {
		e.s.deliverLocal(e.p, proc.DownMsg{Ref: ref, PID: pid, Reason: proc.NoConnection})
		return ref
	}
	if t == nil || !t.alive {
		e.s.send(pid, e.p.pid, proc.DownMsg{Ref: ref, PID: pid, Reason: proc.NoProc})
		return ref
	}
	t.watchers[ref] = watcher{pid: e.p.pid}
	e.p.watching[ref] = pid
	return ref
}

// MonitorName monitors the process with name on node, resolved at once.
func (e *env) MonitorName(node, name string) proc.Ref {
	if pid, ok := e.s.resolve(molecule.Remote{Node: node, Name: name}); ok {
		return e.Monitor(pid)
	}
	ref := e.p.node.alloc.MakeRef()
	e.s.deliverLocal(e.p, proc.DownMsg{Ref: ref, Reason: e.s.unknownName(e.p.pid, node)})
	return ref
}

// unknownName is the reason a name of node is not found from the process
// from: the node is unreachable, or nothing has the name.
func (s *Sim) unknownName(from proc.PID, node string) error {
	if n := s.nodes[node]; n == nil || !n.up || from.Node() != node && s.cuts[[2]string{from.Node(), node}] {
		return proc.NoConnection
	}
	return proc.NoProc
}

func (e *env) Demonitor(ref proc.Ref) {
	if target, ok := e.p.watching[ref]; ok {
		delete(e.p.watching, ref)
		if t := e.s.byPID[target]; t != nil {
			delete(t.watchers, ref)
		}
	}
	isDown := func(msg any) bool {
		d, ok := msg.(proc.DownMsg)
		return ok && d.Ref == ref
	}
	e.p.mailbox = slices.DeleteFunc(e.p.mailbox, isDown)
	for _, l := range e.s.links {
		if l.to == e.p.pid {
			l.queue = slices.DeleteFunc(l.queue, func(f flight) bool { return isDown(f.msg) })
		}
	}
}

func (e *env) Link(pid proc.PID) {
	if pid == e.p.pid {
		return
	}
	t := e.s.byPID[pid]
	if !e.s.reachable(e.p.pid, pid) {
		e.s.signalLocal(e.p, pid, proc.NoConnection)
		return
	}
	if t == nil || !t.alive {
		e.s.signal(e.p, pid, proc.NoProc, false)
		return
	}
	e.p.linked[pid] = true
	t.linked[e.p.pid] = true
}

func (e *env) Unlink(pid proc.PID) {
	delete(e.p.linked, pid)
	if t := e.s.byPID[pid]; t != nil {
		delete(t.linked, e.p.pid)
	}
}

func (e *env) TrapExit(on bool) { e.p.trap = on }

func (e *env) Exit(to proc.PID, reason error) {
	if t := e.s.byPID[to]; t != nil && t.alive && e.s.reachable(e.p.pid, to) {
		e.s.signal(t, e.p.pid, reason, false)
	}
}

func (e *env) Ack(err error) {
	e.p.acked = true
	if err != nil {
		e.p.ackErr = err
	}
}

// StartLink starts a child of a behaviour, on the node of the process,
// and runs the simulation until it has started.
func (e *env) StartLink(child gen.Starter) (proc.PID, error) {
	c, ok := child.(gen.Child)
	if !ok {
		return proc.PID{}, ErrNotSimulated
	}
	return e.s.start(e.p.node, e.p, fmt.Sprintf("%T", child), func(env gen.Env) (gen.Runner, any, string) {
		r, args, opts := c.Simulate(env)
		l, _ := opts.Name.(molecule.Local)
		return r, args, string(l)
	})
}

func (e *env) MonitorNodes(on bool) {
	if !on {
		delete(e.s.nodeWatchers, e.p.pid)
		return
	}
	if e.s.nodeWatchers[e.p.pid] {
		return
	}
	e.s.nodeWatchers[e.p.pid] = true
	for _, other := range e.s.nodeNames() {
		if other != e.p.node.name && e.s.connected(e.p.node.name, other) {
			e.s.deliverLocal(e.p, proc.NodeUp{Node: other})
		}
	}
}

func (e *env) Now() time.Time { return e.s.now }

func (e *env) SendAfter(d time.Duration, msg any) func() {
	e.s.seq++
	t := &simTimer{at: e.s.now.Add(d), seq: e.s.seq, pid: e.p.pid, msg: msg}
	heap.Push(&e.s.timers, t)
	return func() { t.cancelled = true }
}

func (e *env) Request(pid proc.PID, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	ref, _ := e.s.newAlias(e.p.pid, pid, reply)
	return ref, func() { e.s.releaseAlias(ref) }
}

// RequestName requests the process with name on node, resolved at once.
func (e *env) RequestName(node, name string, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	if pid, ok := e.s.resolve(molecule.Remote{Node: node, Name: name}); ok {
		return e.Request(pid, reply)
	}
	ref, _ := e.s.newAlias(e.p.pid, e.p.pid, reply) // watching itself: never down
	e.s.answer(ref, e.p.pid, proc.AliasMsg{Down: true, Reason: e.s.unknownName(e.p.pid, node)})
	return ref, func() { e.s.releaseAlias(ref) }
}

func (e *env) Logger() *slog.Logger { return e.s.logger }
