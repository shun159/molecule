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

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// DefaultCallTimeout bounds the virtual time a Call waits, as
// gen_server:call does by default.
const DefaultCallTimeout = 5 * time.Second

// Sim is a simulated node running behaviours in one goroutine, in an order
// decided by its seed, on a virtual clock.
type Sim struct {
	rng    *rand.Rand
	node   *proc.Node // allocates PIDs and references
	logger *slog.Logger
	now    time.Time

	procs []*process // in creation order
	byPID map[proc.PID]*process
	names map[string]proc.PID

	links  []*link // in creation order, possibly empty
	byLink map[[2]proc.PID]*link

	aliases map[proc.Ref]*alias
	timers  timerHeap
	seq     uint64

	trace []Event

	// CallTimeout bounds the virtual time a Call waits.
	CallTimeout time.Duration
}

// process is a simulated process.
type process struct {
	pid, parent proc.PID
	runner      gen.Runner
	mailbox     []any
	alive       bool
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
	queue    []any
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
		rng:         rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		node:        proc.NewNode("sim", proc.WithCreation(1)),
		logger:      slog.New(slog.DiscardHandler),
		now:         time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		byPID:       make(map[proc.PID]*process),
		names:       make(map[string]proc.PID),
		byLink:      make(map[[2]proc.PID]*link),
		aliases:     make(map[proc.Ref]*alias),
		CallTimeout: DefaultCallTimeout,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Now returns the virtual time.
func (s *Sim) Now() time.Time { return s.now }

// SpawnOption configures Spawn.
type SpawnOption func(*process)

// Named registers the process under name, as a gen.Local name.
func Named(name string) SpawnOption { return func(p *process) { p.name = name } }

// Spawn starts b in a new simulated process and runs its Init, as gen.Start
// does. An error from Init is returned, the process exiting with it.
func Spawn[S any](s *Sim, b gen.Behaviour[S], args any, opts ...SpawnOption) (proc.PID, error) {
	p := &process{
		pid:      s.node.NewPID(),
		alive:    true,
		linked:   make(map[proc.PID]bool),
		watchers: make(map[proc.Ref]watcher),
		watching: make(map[proc.Ref]proc.PID),
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.name != "" {
		if pid, ok := s.names[p.name]; ok {
			return proc.PID{}, &gen.AlreadyStartedError{PID: pid}
		}
		s.names[p.name] = p.pid
	}
	s.procs = append(s.procs, p)
	s.byPID[p.pid] = p
	s.record(Event{Kind: Spawned, To: p.pid, Msg: fmt.Sprintf("%T", b)})

	p.runner = gen.NewRunner(b, &env{s, p})
	if err := p.runner.Init(args); err != nil {
		reason := err
		if errors.Is(err, gen.ErrIgnore) {
			reason = proc.Normal
		}
		s.exit(p, reason)
		return proc.PID{}, err
	}
	if done, reason := p.runner.Flush(); done {
		s.exit(p, reason)
	}
	return p.pid, nil
}

// Send sends msg to the process at to, from outside the simulation.
func (s *Sim) Send(to gen.Dest, msg any) {
	if pid, ok := s.resolve(to); ok {
		s.send(proc.PID{}, pid, msg)
	}
}

// Cast casts req to the server at to, as gen.SendCast does.
func (s *Sim) Cast(to gen.Dest, req any) { s.Send(to, gen.CastMsg{Req: req}) }

// Call calls the server at to and runs the simulation until the reply
// arrives, as gen.Call does. Time passes, timers firing, while it waits,
// up to CallTimeout, after which Call returns context.DeadlineExceeded.
func (s *Sim) Call(to gen.Dest, req any) (any, error) {
	pid, ok := s.resolve(to)
	if !ok {
		return nil, &gen.ExitError{To: to, Reason: proc.NoProc}
	}
	ref, a := s.newAlias(proc.PID{}, pid, nil)
	s.send(proc.PID{}, pid, gen.CallMsg{From: gen.From{Tag: ref}, Req: req})

	deadline := s.now.Add(s.CallTimeout)
	for !a.done {
		if s.Step() {
			continue
		}
		if !s.fireTimer(deadline) {
			s.releaseAlias(ref)
			s.now = deadline
			return nil, context.DeadlineExceeded
		}
	}
	if a.result.Down {
		return nil, &gen.ExitError{To: to, Reason: a.result.Reason}
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

// WhereIs returns the process registered under name.
func (s *Sim) WhereIs(name string) (proc.PID, bool) {
	pid, ok := s.names[name]
	return pid, ok
}

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
		if p.alive && len(p.mailbox) > 0 {
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

func (s *Sim) resolve(dest gen.Dest) (proc.PID, bool) {
	switch d := dest.(type) {
	case proc.PID:
		return d, !d.IsZero()
	case gen.Local:
		pid, ok := s.names[string(d)]
		return pid, ok
	}
	return dest.WhereIs(s.node)
}

// send puts msg in flight from one process to another.
func (s *Sim) send(from, to proc.PID, msg any) {
	s.record(Event{Kind: Sent, From: from, To: to, Msg: msg})
	key := [2]proc.PID{from, to}
	l := s.byLink[key]
	if l == nil {
		l = &link{from: from, to: to}
		s.byLink[key] = l
		s.links = append(s.links, l)
	}
	l.queue = append(l.queue, msg)
}

// deliver moves the first message in flight on l to its mailbox.
func (s *Sim) deliver(l *link) {
	msg := l.queue[0]
	l.queue[0] = nil
	l.queue = l.queue[1:]
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
	if done, reason := p.runner.Deliver(msg); done {
		s.exit(p, reason)
	}
}

// exit makes p dead with reason and tells its links and monitors.
func (s *Sim) exit(p *process, reason error) {
	if !p.alive {
		return
	}
	p.alive = false
	p.runner.Abort()
	p.mailbox = nil
	s.record(Event{Kind: Exited, To: p.pid, Msg: reason})
	if p.name != "" && s.names[p.name] == p.pid {
		delete(s.names, p.name)
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
	ref := s.node.MakeRef()
	a := &alias{owner: owner, target: target, reply: reply}
	s.aliases[ref] = a
	if t := s.byPID[target]; t != nil && t.alive {
		t.watchers[ref] = watcher{alias: true}
	} else {
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

func (e *env) Resolve(dest gen.Dest) (proc.PID, bool) { return e.s.resolve(dest) }

func (e *env) Send(to proc.PID, msg any) { e.s.send(e.p.pid, to, msg) }

// SendName sends to a name of the simulated node; a simulation has no
// other node, and what is sent to one is lost.
func (e *env) SendName(node, name string, msg any) {
	if node != e.s.node.Name() {
		return
	}
	if pid, ok := e.s.resolve(gen.Local(name)); ok {
		e.s.send(e.p.pid, pid, msg)
	}
}

func (e *env) SendAlias(ref proc.Ref, msg any) {
	e.s.answer(ref, e.p.pid, proc.AliasMsg{Msg: msg})
}

func (e *env) Monitor(pid proc.PID) proc.Ref {
	ref := e.s.node.MakeRef()
	t := e.s.byPID[pid]
	if t == nil || !t.alive {
		e.s.send(pid, e.p.pid, proc.DownMsg{Ref: ref, PID: pid, Reason: proc.NoProc})
		return ref
	}
	t.watchers[ref] = watcher{pid: e.p.pid}
	e.p.watching[ref] = pid
	return ref
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
			l.queue = slices.DeleteFunc(l.queue, isDown)
		}
	}
}

func (e *env) Link(pid proc.PID) {
	if pid == e.p.pid {
		return
	}
	t := e.s.byPID[pid]
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

func (e *env) Logger() *slog.Logger { return e.s.logger }
