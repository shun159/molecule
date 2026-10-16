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

	"github.com/shun159/molecule/actor"
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
	held         map[[2]string]bool // pairs of nodes holding what they send each other
	paused       map[string]bool
	skews        map[string]time.Duration // how far the clock of each node is off
	told         map[[2]string]bool       // x, y: x told y silent
	silences     map[[2]string]*simTimer  // x, y: to tell x y silent

	procs []*process // in creation order
	byPID map[proc.PID]*process

	links  []*link // in creation order, possibly empty
	byLink map[[2]proc.PID]*link

	aliases map[proc.Ref]*alias
	timers  timerHeap
	seq     uint64

	trace []Event

	noPurity bool
	events   func(any) // see WithEvents
	choices  []byte    // see WithChoices, those left

	// CallTimeout bounds the virtual time a Call waits.
	CallTimeout time.Duration
	// Loss is the probability that a message between two nodes is lost,
	// beyond the losses of partitions and crashes. Erlang loses none
	// while connected; a protocol that must bear it, as Raft, is tested
	// with it.
	Loss float64
	// Silent is how long a node goes unheard, paused or silenced, before
	// the others are told it silent, as dist.Config.Silent.
	Silent time.Duration
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
	maxMessages int // see actor.WithMaxMessages
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

// WithChoices makes the choices of the run -- the step taken next, the
// message lost -- read from data, rather than drawn from the seed, until
// data is used up, the seed then drawing the rest. Bytes go only to choices
// among several: a byte for one among up to 256.
//
// It is for fuzzing the order of events: given the bytes of a fuzz
// target, the fuzzer mutates the schedule, a byte a choice, toward
// runs covering new code, and keeps a schedule failing in testdata/fuzz,
// replayed by go test from then on.
//
//	func FuzzTree(f *testing.F) {
//		f.Add([]byte{})
//		f.Fuzz(func(t *testing.T, schedule []byte) {
//			s := gensim.New(1, gensim.WithChoices(schedule))
//			...
//		})
//	}
func WithChoices(data []byte) Option { return func(s *Sim) { s.choices = data } }

// WithEvents makes the simulation hand h the events of its processes, as
// proc.WithEvents does those of a node: proc.Crashed, proc.Overflowed, and
// those its behaviours emit. h is called as the event happens, in the
// order of the run.
func WithEvents(h func(event any)) Option { return func(s *Sim) { s.events = h } }

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
		held:         make(map[[2]string]bool),
		paused:       make(map[string]bool),
		skews:        make(map[string]time.Duration),
		told:         make(map[[2]string]bool),
		silences:     make(map[[2]string]*simTimer),
		byPID:        make(map[proc.PID]*process),
		byLink:       make(map[[2]proc.PID]*link),
		aliases:      make(map[proc.Ref]*alias),
		CallTimeout:  DefaultCallTimeout,
		Silent:       DefaultSilent,
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
		s.listen()
	}
	return n
}

// Now returns the virtual time.
func (s *Sim) Now() time.Time { return s.now }

// SpawnOption configures Spawn.
type SpawnOption func(*process)

// Named registers the process under name, as a actor.Local name of its
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
	return s.start(node, nil, fmt.Sprintf("%T", b), func(e gen.Env) (gen.Runner, any, actor.StartOptions) {
		var o actor.StartOptions
		if p.name != "" {
			o.Name = actor.Local(p.name)
		}
		return gen.NewRunner(b, e), args, o
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
	return s.start(node, nil, fmt.Sprintf("%T", c), func(e gen.Env) (gen.Runner, any, actor.StartOptions) {
		r, args, o := c.Simulate(e)
		if p.name != "" {
			o.Name = actor.Local(p.name)
		}
		return r, args, o
	})
}

// ErrNotSimulated is the error of starting a child a simulation cannot
// run: one started by a function, rather than a behaviour.
var ErrNotSimulated = errors.New("gensim: child started by a function, which cannot be simulated")

// start starts a process on node, linked to parent if any, running the
// runner newRunner makes, started with the options it tells: registered
// under its local name, if any. It runs the simulation until the process
// has started, as Start and StartLink wait.
func (s *Sim) start(node *node, parent *process, what string, newRunner func(gen.Env) (gen.Runner, any, actor.StartOptions)) (proc.PID, error) {
	if !node.up {
		return proc.PID{}, ErrNodeDown
	}
	p := newProcess(node)
	runner, args, o := newRunner(&env{s, p})
	p.maxMessages = o.MaxMessages
	if err := node.register(p, o.Name); err != nil {
		return proc.PID{}, err
	}
	s.add(p, parent, what)
	if err := s.init(p, runner, args); err != nil {
		return proc.PID{}, err
	}
	s.runUntilAcked(p)
	return p.started()
}

func newProcess(node *node) *process {
	return &process{
		node:     node,
		pid:      node.alloc.NewPID(),
		alive:    true,
		linked:   make(map[proc.PID]bool),
		watchers: make(map[proc.Ref]watcher),
		watching: make(map[proc.Ref]proc.PID),
	}
}

// register registers p under its local name, if any: one another process
// has fails the start with AlreadyStartedError.
func (node *node) register(p *process, name actor.Name) error {
	l, _ := name.(actor.Local)
	if l == "" {
		return nil
	}
	if pid, ok := node.names[string(l)]; ok {
		return &actor.AlreadyStartedError{PID: pid}
	}
	node.names[string(l)] = p.pid
	p.name = string(l)
	return nil
}

// add adds p to the simulation, linked to parent, if any.
func (s *Sim) add(p, parent *process, what string) {
	if parent != nil {
		p.parent = parent.pid
		p.linked[parent.pid] = true
		parent.linked[p.pid] = true
	}
	s.procs = append(s.procs, p)
	s.byPID[p.pid] = p
	s.record(Event{Kind: Spawned, To: p.pid, Msg: what})
}

// init runs Init on p, then what it has the runner handle at once. A
// process ignoring its start ends normally, with the start failing.
func (s *Sim) init(p *process, runner gen.Runner, args any) error {
	p.runner = runner
	p.busy = true
	if err := runner.Init(args); err != nil {
		p.busy = false
		reason := err
		if errors.Is(err, actor.ErrIgnore) {
			reason = proc.Normal
		}
		s.end(p, reason)
		return err
	}
	done, reason := runner.Flush()
	p.busy = false
	if done {
		s.end(p, reason)
	}
	return nil
}

// runUntilAcked runs the simulation on while the start of p is held back,
// until it is told over, or p dies, or nothing is left to run.
func (s *Sim) runUntilAcked(p *process) {
	for p.alive && !p.acked {
		if !s.Step() && !s.fireTimer(s.now.Add(s.CallTimeout)) {
			return
		}
	}
}

// started returns the outcome of the start of p.
func (p *process) started() (proc.PID, error) {
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
func (s *Sim) Send(to actor.Dest, msg any) {
	if pid, ok := s.resolve(to); ok {
		s.send(proc.PID{}, pid, msg)
	}
}

// Cast casts req to the server at to, as actor.SendCast does.
func (s *Sim) Cast(to actor.Dest, req any) { s.Send(to, actor.CastMsg{Req: req}) }

// Call calls the server at to and runs the simulation until the reply
// arrives, as actor.Call does. Time passes, timers firing, while it waits,
// up to CallTimeout, after which Call returns context.DeadlineExceeded, the
// server told with a actor.CallAbandoned, as actor.Call does.
func (s *Sim) Call(to actor.Dest, req any) (any, error) {
	pid, ok := s.resolve(to)
	if !ok {
		return nil, &actor.ExitError{To: to, Reason: proc.NoProc}
	}
	ref, a := s.newAlias(proc.PID{}, pid, nil)
	s.send(proc.PID{}, pid, actor.CallMsg{From: actor.From{Tag: ref}, Req: req})

	deadline := s.now.Add(s.CallTimeout)
	for !a.done {
		if s.Step() {
			continue
		}
		if !s.fireTimer(deadline) {
			s.releaseAlias(ref)
			s.now = deadline
			s.send(proc.PID{}, pid, actor.CallAbandoned{From: actor.From{Tag: ref}})
			return nil, context.DeadlineExceeded
		}
	}
	if a.result.Down {
		return nil, &actor.ExitError{To: to, Reason: a.result.Reason}
	}
	return a.result.Msg, nil
}

// ready reports whether p may handle a message now: alive, with one in
// its mailbox, on a node not paused. A process busy starting another
// handles nothing meanwhile.
func (s *Sim) ready(p *process) bool {
	idle := p.alive && !p.busy
	return idle && len(p.mailbox) > 0 && !s.paused[p.node.name]
}

// alive returns the process pid, if alive.
func (s *Sim) alive(pid proc.PID) *process {
	if p := s.byPID[pid]; p != nil && p.alive {
		return p
	}
	return nil
}

// Exit sends an exit signal with reason to the process pid from outside
// the simulation, as proc.Self.Exit does: proc.Kill kills it.
func (s *Sim) Exit(pid proc.PID, reason error) {
	if p := s.alive(pid); p != nil {
		s.signal(p, proc.PID{}, reason, false)
	}
}

// Alive reports whether the process pid is alive.
func (s *Sim) Alive(pid proc.PID) bool {
	p := s.byPID[pid]
	return p != nil && p.alive
}

// WhereIs returns the process registered under name on DefaultNode, or
// on another node with a name of the form actor.Remote takes, see Resolve.
func (s *Sim) WhereIs(name string) (proc.PID, bool) {
	pid, ok := s.def.names[name]
	return pid, ok
}

// Resolve returns the process at dest, as the driver sees it: a
// actor.Local name is of DefaultNode, and a actor.Remote name of any node.
func (s *Sim) Resolve(dest actor.Dest) (proc.PID, bool) { return s.resolve(dest) }

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
		if len(l.queue) > 0 && s.flows(l) {
			choices = append(choices, func() { s.deliver(l) })
		}
	}
	for _, p := range s.procs {
		if s.ready(p) {
			choices = append(choices, func() { s.handle(p) })
		}
	}
	if len(choices) == 0 {
		return false
	}
	choices[s.choose(len(choices))]()
	return true
}

// choose returns a choice among n, read from the choices given, if any
// are left, or drawn from the seed.
func (s *Sim) choose(n int) int {
	if len(s.choices) == 0 {
		return s.rng.IntN(n)
	}
	v := 0
	for m := 1; m < n && len(s.choices) > 0; m <<= 8 {
		v = v<<8 | int(s.choices[0])
		s.choices = s.choices[1:]
	}
	return v % n
}

// chance reports true with probability p, as choose chooses.
func (s *Sim) chance(p float64) bool {
	if len(s.choices) == 0 {
		return s.rng.Float64() < p
	}
	b := s.choices[0]
	s.choices = s.choices[1:]
	return float64(b)/256 < p
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

func (s *Sim) resolve(dest actor.Dest) (proc.PID, bool) {
	switch d := dest.(type) {
	case proc.PID:
		return d, !d.IsZero()
	case actor.Local:
		pid, ok := s.def.names[string(d)]
		return pid, ok
	case actor.Remote:
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
	if s.lost(from, to) {
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
	if p := s.alive(l.to); p != nil {
		s.enqueue(p, msg)
	}
}

// enqueue puts msg in the mailbox of p, and kills p if that is past its
// limit, as proc.Self.SetMaxMessages does.
func (s *Sim) enqueue(p *process, msg any) {
	p.mailbox = append(p.mailbox, msg)
	if p.maxMessages > 0 && len(p.mailbox) > p.maxMessages {
		s.emit(proc.Overflowed{PID: p.pid, Name: p.name, Label: p.runner.Label(), Len: len(p.mailbox)})
		s.exit(p, proc.MailboxFull)
	}
}

// end makes p dead with reason, its behaviour ended: crashed, if reason
// is abnormal, as a process whose function failed.
func (s *Sim) end(p *process, reason error) {
	if p.alive && proc.IsAbnormal(reason) {
		s.emit(proc.Crashed{PID: p.pid, Name: p.name, Label: p.runner.Label(), Reason: reason})
	}
	s.exit(p, reason)
}

func (s *Sim) emit(event any) {
	if s.events != nil {
		s.events(event)
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
		s.end(p, reason)
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
		if q := s.alive(pid); q != nil {
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
		if q := s.alive(w.pid); q != nil {
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
	if t.fn != nil {
		t.fn()
		return true
	}
	if p := s.alive(t.pid); p != nil {
		s.record(Event{Kind: Fired, To: t.pid, Msg: t.msg})
		s.enqueue(p, t.msg)
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

// simTimer is a message to deliver at a virtual time, or a function of
// the simulation to run then.
type simTimer struct {
	at        time.Time
	seq       uint64
	pid       proc.PID
	msg       any
	fn        func()
	cancelled bool
}

// after runs fn after d of virtual time.
func (s *Sim) after(d time.Duration, fn func()) *simTimer {
	s.seq++
	t := &simTimer{at: s.now.Add(d), seq: s.seq, fn: fn}
	heap.Push(&s.timers, t)
	return t
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

// Resolve resolves dest as on the node of the process: a actor.Remote name
// of another node does not resolve, as in proc.
func (e *env) Resolve(dest actor.Dest) (proc.PID, bool) {
	switch d := dest.(type) {
	case actor.Local:
		pid, ok := e.p.node.names[string(d)]
		return pid, ok
	case actor.Remote:
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
	if pid, ok := e.s.resolve(actor.Remote{Node: node, Name: name}); ok {
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
	if pid, ok := e.s.resolve(actor.Remote{Node: node, Name: name}); ok {
		return e.Monitor(pid)
	}
	ref := e.p.node.alloc.MakeRef()
	e.s.deliverLocal(e.p, proc.DownMsg{Ref: ref, Reason: e.s.unknownName(e.p.pid, node)})
	return ref
}

// lost reports whether a message from from to to is lost, at the rate Loss:
// between processes of two nodes only. The chance is drawn last, so that
// a run takes the same draws, whatever it sends within a node.
func (s *Sim) lost(from, to proc.PID) bool {
	betweenNodes := !from.IsZero() && from.Node() != to.Node()
	return s.Loss > 0 && betweenNodes && s.chance(s.Loss)
}

// unknownName is the reason a name of node is not found from the process
// from: the node is unreachable, or nothing has the name.
func (s *Sim) unknownName(from proc.PID, node string) error {
	n := s.nodes[node]
	down := n == nil || !n.up
	cutOff := from.Node() != node && s.cuts[[2]string{from.Node(), node}]
	if down || cutOff {
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
	if t := e.s.alive(to); t != nil && e.s.reachable(e.p.pid, to) {
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
	return e.s.start(e.p.node, e.p, fmt.Sprintf("%T", child), c.Simulate)
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
	for _, other := range e.s.nodeNames() {
		if e.s.told[[2]string{e.p.node.name, other}] {
			e.s.deliverLocal(e.p, proc.NodeSilent{Node: other})
		}
	}
}

func (e *env) Now() time.Time { return e.s.now.Add(e.s.skews[e.p.pid.Node()]) }

func (e *env) SendAfter(d time.Duration, msg any) func() {
	e.s.seq++
	t := &simTimer{at: e.s.now.Add(d), seq: e.s.seq, pid: e.p.pid, msg: msg}
	heap.Push(&e.s.timers, t)
	return func() { t.cancelled = true }
}

// Async runs run at once, to completion, and sends its outcome to the
// process, in flight as any message: see the package doc.
func (e *env) Async(run func(context.Context) (any, error), done func(any, error) any) func() {
	ctx, cancel := context.WithCancel(context.Background())
	v, err := gen.RunAsync(ctx, run)
	e.s.send(e.p.pid, e.p.pid, done(v, err))
	return cancel
}

func (e *env) Request(pid proc.PID, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	ref, _ := e.s.newAlias(e.p.pid, pid, reply)
	return ref, func() { e.s.releaseAlias(ref) }
}

// RequestName requests the process with name on node, resolved at once.
func (e *env) RequestName(node, name string, reply func(proc.Ref, proc.AliasMsg) any) (proc.Ref, func()) {
	if pid, ok := e.s.resolve(actor.Remote{Node: node, Name: name}); ok {
		return e.Request(pid, reply)
	}
	ref, _ := e.s.newAlias(e.p.pid, e.p.pid, reply) // watching itself: never down
	e.s.answer(ref, e.p.pid, proc.AliasMsg{Down: true, Reason: e.s.unknownName(e.p.pid, node)})
	return ref, func() { e.s.releaseAlias(ref) }
}

func (e *env) Logger() *slog.Logger { return e.s.logger }

func (e *env) Emit(event any) { e.s.emit(event) }
