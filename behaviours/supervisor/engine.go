package supervisor

import (
	"errors"
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// A supervisor, static or dynamic, is a behaviour of gen: pure functions of
// its state and the messages it gets, whose effects start and stop its
// children. What it does takes steps, waiting for a child to start or to
// stop: the steps are queued as operations, run in order, and the exits of
// children arriving meanwhile are handled once the queue is empty. Being
// pure, it runs the same in a process and in gensim.

// sup is the behaviour, static or dynamic.
type sup struct {
	dynamic     bool
	strategy    Strategy
	max         int
	period      time.Duration
	maxChildren int
	specs       []ChildSpec // of a static supervisor
}

// child is a child of a supervisor, by the order of its start.
type child struct {
	key      uint64
	spec     ChildSpec
	pid      proc.PID // zero when not running
	restarts int      // the times a restart started it again
}

type opKind int

const (
	opStart  opKind = iota // start the child key
	opStop                 // stop the children keys, at once, and wait for them
	opRemove               // forget the child key
	opReply                // reply value to from
	opAck                  // end the start of the supervisor, with err
	opFinish               // stop the supervisor, with err
)

type op struct {
	kind    opKind
	keys    []uint64
	initial bool // a start of Init: its failure fails the supervisor
	restart bool // a start of a restart: its failure is retried
	call    bool // a start asked by StartChild, to reply to from
	from    actor.From
	value   any
	err     error
}

// state is the state of a supervisor. It is copied as a whole before being
// changed, so that the state a callback gets is never changed.
type state struct {
	self      proc.PID
	children  []child
	nextKey   uint64
	restarts  int // restarts within the period
	timerSeq  uint64
	ops       []op
	waitStart bool       // for the Started of the first op
	stopping  []proc.PID // the children stopped, waited for
	exits     []proc.ExitMsg
	final     bool // stopping for good
	stopCalls []actor.From
}

func (s state) clone() state {
	s.children = slices.Clone(s.children)
	s.ops = slices.Clone(s.ops)
	s.stopping = slices.Clone(s.stopping)
	s.exits = slices.Clone(s.exits)
	s.stopCalls = slices.Clone(s.stopCalls)
	return s
}

// Messages of a supervisor to itself, and its timers.
type (
	startTag struct{ key uint64 }
	killKey  struct{ pid proc.PID }
	kill     struct{ pid proc.PID }
	// expired ends a restart in the period, under expireKey.
	expireKey struct{ seq uint64 }
	expired   struct{}
	// retry asks to try again to restart a child that failed to start
	// during a restart, as OTP does.
	retry struct{ key uint64 }
)

func (b sup) Init(self proc.PID, _ any) (state, []actor.Effect, error) {
	s := state{self: self}
	effs := []actor.Effect{actor.TrapExit{On: true}}
	if b.dynamic {
		return s, effs, nil
	}
	if err := validate(Spec{Strategy: b.strategy, Children: b.specs}); err != nil {
		return s, nil, err
	}
	for _, spec := range b.specs {
		s.nextKey++
		s.children = append(s.children, child{key: s.nextKey, spec: spec})
		s.ops = append(s.ops, op{kind: opStart, keys: []uint64{s.nextKey}, initial: true})
	}
	s.ops = append(s.ops, op{kind: opAck})
	effs = append(effs, gen.AckLater{})
	s, more := b.advance(s)
	return s, append(effs, more...), nil
}

func (b sup) Handle(s state, msg gen.Msg) (state, []actor.Effect) {
	s = s.clone()
	var effs []actor.Effect
	switch m := msg.(type) {
	case gen.ContinueMsg:
		if st, ok := m.Msg.(gen.Started); ok {
			effs = b.started(&s, st)
		}
	case actor.CallMsg:
		effs = b.call(&s, m.From, m.Req)
	case gen.InfoMsg:
		effs = b.info(&s, m.Msg)
	}
	s, more := b.advance(s)
	return s, append(effs, more...)
}

// ParentExit stops the children, then the supervisor, with the reason of
// the parent, as a supervisor does.
func (b sup) ParentExit(s state, reason error) (state, []actor.Effect) {
	s = s.clone()
	b.finish(&s, reason)
	return b.advance(s)
}

func (sup) Terminate(state, error) []actor.Effect { return nil }

// Label tells what the process is.
func (b sup) Label() string {
	if b.dynamic {
		return "dynamic supervisor"
	}
	return "supervisor"
}

func (b sup) info(s *state, msg any) []actor.Effect {
	switch m := msg.(type) {
	case proc.ExitMsg:
		return b.exit(s, m)
	case kill:
		if slices.Contains(s.stopping, m.pid) {
			return actor.Do(actor.Exit{To: m.pid, Reason: proc.Kill})
		}
	case expired:
		s.restarts--
	case retry:
		if i := s.byKey(m.key); s.restartable(i) {
			return b.restart(s, i)
		}
	}
	return nil
}

// exit handles the exit of a process linked: a child stopped, waited for;
// a child running, handled now, or once the operations under way are done.
func (b sup) exit(s *state, m proc.ExitMsg) []actor.Effect {
	if i := slices.Index(s.stopping, m.From); i >= 0 {
		s.stopping = slices.Delete(s.stopping, i, i+1)
		return actor.Do(actor.CancelTimer{Key: killKey{m.From}})
	}
	if s.byPID(m.From) < 0 {
		return nil // a child already stopped or replaced
	}
	if s.busy() {
		s.exits = append(s.exits, m)
		return nil
	}
	return b.exited(s, m)
}

// restartable reports whether the child at i, if any, is still down, for
// a retry to restart it: none is once the supervisor stops for good.
func (s state) restartable(i int) bool {
	return i >= 0 && s.children[i].pid.IsZero() && !s.final
}

func (s state) busy() bool { return s.waitStart || len(s.stopping) > 0 || len(s.ops) > 0 }

// exited handles the exit of a running child.
func (b sup) exited(s *state, m proc.ExitMsg) []actor.Effect {
	i := s.byPID(m.From)
	if i < 0 {
		return nil
	}
	c := &s.children[i]
	id := b.childID(*c)
	effs := reportTerminated(s.self, id, m.From, c.spec.Restart, m.Reason)
	c.pid = proc.PID{}
	if !shouldRestart(c.spec.Restart, m.Reason) {
		if b.dynamic || c.spec.Restart == Temporary {
			s.children = slices.Delete(s.children, i, i+1)
		}
		return effs
	}
	final := s.final
	effs = append(effs, b.restart(s, i)...)
	return append(effs, s.restartEvent(final, id, m)...)
}

// restartEvent is the event of the restart of the child id, after its exit
// m: it restarted, or the supervisor gave up on it, now final, unless it
// was before.
func (s state) restartEvent(wasFinal bool, id string, m proc.ExitMsg) []actor.Effect {
	switch {
	case !s.final:
		return actor.Do(gen.Emit{Event: Restarted{Supervisor: s.self, Child: id, PID: m.From, Reason: m.Reason}})
	case !wasFinal:
		return actor.Do(gen.Emit{Event: GaveUp{Supervisor: s.self, Child: id, PID: m.From, Reason: m.Reason}})
	}
	return nil
}

// restart restarts the child at i, not running, and those the strategy
// involves, unless restarts are too many: then the supervisor gives up.
func (b sup) restart(s *state, i int) []actor.Effect {
	if s.restarts+1 > b.max {
		effs := reportShutdown(s.self, ErrMaxIntensity)
		b.finish(s, ErrMaxIntensity)
		return effs
	}
	s.restarts++
	s.timerSeq++
	effs := actor.Do(actor.StartTimer{Key: expireKey{s.timerSeq}, After: b.period, Msg: expired{}})
	if b.dynamic {
		s.ops = append(s.ops, op{kind: opStart, keys: []uint64{s.children[i].key}, restart: true})
		return effs
	}
	stop, start := plan(b.strategy, i, len(s.children))
	for _, j := range stop {
		s.ops = append(s.ops, op{kind: opStop, keys: []uint64{s.children[j].key}})
	}
	for _, j := range start {
		s.ops = append(s.ops, op{kind: opStart, keys: []uint64{s.children[j].key}, restart: true})
	}
	return effs
}

// finish stops the children, in reverse order, or all at once for a
// dynamic supervisor, then the supervisor, with reason. What was under
// way, but a stop waited for, is dropped.
func (b sup) finish(s *state, reason error) {
	if s.final {
		return
	}
	s.final = true
	s.exits = nil
	s.ops = append(b.stopAll(s), op{kind: opFinish, err: reason})
}

func (b sup) stopAll(s *state) []op {
	var ops []op
	if b.dynamic {
		var keys []uint64
		for _, c := range s.children {
			keys = append(keys, c.key)
		}
		return []op{{kind: opStop, keys: keys}}
	}
	for i := len(s.children) - 1; i >= 0; i-- {
		ops = append(ops, op{kind: opStop, keys: []uint64{s.children[i].key}})
	}
	return ops
}

// started handles the outcome of the start of the child of the first op.
func (b sup) started(s *state, st gen.Started) []actor.Effect {
	if !s.waitStart || len(s.ops) == 0 {
		return nil
	}
	s.waitStart = false
	o := s.ops[0]
	s.ops = s.ops[1:]
	i := s.byKey(o.keys[0])
	switch {
	case errors.Is(st.Err, actor.ErrIgnore):
		return b.ignored(s, i, o, st.Err)
	case st.Err != nil:
		return b.startFailed(s, i, o, st.Err)
	default:
		return b.running(s, i, o, st.PID)
	}
}

// ignored handles the child at i ignoring its start, with err: a dynamic
// supervisor forgets it, and the call starting it gets err.
func (b sup) ignored(s *state, i int, o op, err error) []actor.Effect {
	if b.dynamic {
		s.children = slices.Delete(s.children, i, i+1)
	}
	if o.call {
		return actor.Do(actor.Reply{To: o.from, Value: startResult{err: err}})
	}
	return nil
}

// startFailed handles the child at i failing to start, by o: the start of
// the supervisor fails with it, a call starting it gets the error, and a
// restart is tried again.
func (b sup) startFailed(s *state, i int, o op, err error) []actor.Effect {
	c := s.children[i]
	id := b.childID(c)
	effs := reportStartFailed(s.self, id, err)
	switch {
	case o.initial:
		failed := &StartError{ID: id, Reason: err}
		s.final = true
		s.ops = append(b.stopAll(s), op{kind: opAck, err: failed}, op{kind: opFinish, err: failed})
		return effs
	case o.call:
		s.children = slices.Delete(s.children, i, i+1)
		return append(effs, actor.Reply{To: o.from, Value: startResult{err: err}})
	default:
		return append(effs, s.retryLater(c.key))
	}
}

// retryLater has the restart of the child key tried again: the children
// after it in the restart stay down until the retry restarts them.
func (s *state) retryLater(key uint64) actor.Effect {
	for s.nextRestartStart() {
		s.ops = s.ops[1:]
	}
	return actor.Send{To: s.self, Msg: retry{key}}
}

// nextRestartStart reports whether the next op starts a child of a restart.
func (s state) nextRestartStart() bool {
	return len(s.ops) > 0 && s.ops[0].kind == opStart && s.ops[0].restart
}

// running notes the child at i started by o, running as pid.
func (b sup) running(s *state, i int, o op, pid proc.PID) []actor.Effect {
	c := &s.children[i]
	c.pid = pid
	if o.restart {
		c.restarts++
	}
	effs := reportStarted(s.self, b.childID(*c), pid)
	if o.call {
		effs = append(effs, actor.Reply{To: o.from, Value: startResult{pid: pid}})
	}
	return effs
}

// advance runs the operations queued, until one waits, then the exits that
// came meanwhile.
func (b sup) advance(s state) (state, []actor.Effect) {
	var effs []actor.Effect
	for !s.waiting() {
		if len(s.ops) == 0 {
			if len(s.exits) == 0 || s.final { // none, or none to handle once stopping
				return s, effs
			}
			e := s.exits[0]
			s.exits = s.exits[1:]
			effs = append(effs, b.exited(&s, e)...)
			continue
		}
		more, stopped := b.runOp(&s)
		effs = append(effs, more...)
		if stopped {
			return s, effs
		}
	}
	return s, effs
}

// waiting reports whether an op waits: for a child to start, or for those
// stopped to exit.
func (s state) waiting() bool { return s.waitStart || len(s.stopping) > 0 }

// runOp runs the first op, and reports whether the supervisor stops.
func (b sup) runOp(s *state) ([]actor.Effect, bool) {
	o := s.ops[0]
	switch o.kind {
	case opStart:
		return b.startOp(s, o), false
	case opStop:
		s.ops = s.ops[1:]
		return s.stop(o.keys), false
	case opRemove:
		if i := s.byKey(o.keys[0]); i >= 0 {
			s.children = slices.Delete(s.children, i, i+1)
		}
		s.ops = s.ops[1:]
	case opReply:
		s.ops = s.ops[1:]
		return actor.Do(actor.Reply{To: o.from, Value: o.value}), false
	case opAck:
		s.ops = s.ops[1:]
		return actor.Do(gen.Ack{Err: o.err}), false
	case opFinish:
		return s.finished(o.err), true
	}
	return nil, false
}

// startOp starts the child of o, to wait for its Started, unless it is gone
// or running already, or is forgotten.
func (b sup) startOp(s *state, o op) []actor.Effect {
	i := s.byKey(o.keys[0])
	switch {
	case i < 0 || !s.children[i].pid.IsZero():
		s.ops = s.ops[1:]
	case b.forgotten(s.children[i], o):
		s.children = slices.Delete(s.children, i, i+1)
		s.ops = s.ops[1:]
	default:
		s.waitStart = true
		return actor.Do(gen.StartChild{Child: s.children[i].spec.Start, Tag: startTag{o.keys[0]}})
	}
	return nil
}

// forgotten reports whether c is not restarted by o but forgotten: a
// temporary child of a static supervisor stopped along with a sibling, as
// in OTP.
func (b sup) forgotten(c child, o op) bool {
	return o.restart && c.spec.Restart == Temporary && !b.dynamic
}

// stop stops the children of keys running, at once, for advance to wait
// for their exits.
func (s *state) stop(keys []uint64) []actor.Effect {
	var effs []actor.Effect
	for _, k := range keys {
		i := s.byKey(k)
		if i < 0 || s.children[i].pid.IsZero() {
			continue // gone, or not running
		}
		effs = append(effs, s.stopChild(i)...)
	}
	return effs
}

// stopChild stops the child at i, as its shutdown says: killed at once,
// asked to shut down, or asked and killed after a while.
func (s *state) stopChild(i int) []actor.Effect {
	c := &s.children[i]
	pid := c.pid
	c.pid = proc.PID{}
	s.stopping = append(s.stopping, pid)
	switch d := c.spec.shutdown(); d {
	case Brutal:
		return actor.Do(actor.Exit{To: pid, Reason: proc.Kill})
	case Infinity:
		return actor.Do(actor.Exit{To: pid, Reason: proc.Shutdown})
	default:
		return actor.Do(
			actor.Exit{To: pid, Reason: proc.Shutdown},
			actor.StartTimer{Key: killKey{pid}, After: d, Msg: kill{pid}})
	}
}

// finished stops the supervisor with err, answering the calls that asked it
// to stop.
func (s *state) finished(err error) []actor.Effect {
	var effs []actor.Effect
	for _, from := range s.stopCalls {
		effs = append(effs, actor.Reply{To: from})
	}
	s.ops = nil
	return append(effs, actor.Stop{Reason: err})
}

func (b sup) childID(c child) string {
	if b.dynamic {
		return ""
	}
	return c.spec.ID
}

func (s state) byKey(k uint64) int {
	return slices.IndexFunc(s.children, func(c child) bool { return c.key == k })
}

func (s state) byPID(pid proc.PID) int {
	if pid.IsZero() {
		return -1 // children not running have a zero PID
	}
	return slices.IndexFunc(s.children, func(c child) bool { return c.pid == pid })
}

func (b sup) which(s state) []ChildInfo {
	var infos []ChildInfo
	for _, c := range s.children {
		if b.dynamic && c.pid.IsZero() {
			continue
		}
		infos = append(infos, ChildInfo{ID: b.childID(c), PID: c.pid, Type: c.spec.Type, Restart: c.spec.Restart, Restarts: c.restarts})
	}
	return infos
}

// call handles the calls of the API.
func (b sup) call(s *state, from actor.From, req any) []actor.Effect {
	reply := func(v any) []actor.Effect { return actor.Do(actor.Reply{To: from, Value: v}) }
	switch r := req.(type) {
	case whichChildren:
		return reply(b.which(*s))
	case stopReq:
		s.stopCalls = append(s.stopCalls, from)
		b.finish(s, proc.Shutdown)
	case countChildren:
		return reply(len(s.children))
	case startChild:
		if err := b.mayStart(*s); err != nil {
			return reply(startResult{err: err})
		}
		s.nextKey++
		s.children = append(s.children, child{key: s.nextKey, spec: r.spec})
		s.ops = append(s.ops, op{kind: opStart, keys: []uint64{s.nextKey}, call: true, from: from})
	case terminateChild:
		return b.terminateChild(s, from, r.pid)
	}
	return nil
}

// mayStart tells why a child may not start by StartChild, if it may not.
func (b sup) mayStart(s state) error {
	switch {
	case !b.dynamic:
		return errors.New("supervisor: StartChild of a static supervisor")
	case s.final:
		return proc.Shutdown
	case b.maxChildren > 0 && len(s.children) >= b.maxChildren:
		return ErrMaxChildren
	}
	return nil
}

// terminateChild queues the stop of the child pid, a child of a dynamic
// supervisor, then its removal, then the reply to from.
func (b sup) terminateChild(s *state, from actor.From, pid proc.PID) []actor.Effect {
	i := s.byPID(pid)
	if i < 0 || !b.dynamic {
		return actor.Do(actor.Reply{To: from, Value: ErrNotFound})
	}
	k := s.children[i].key
	s.ops = append(s.ops,
		op{kind: opStop, keys: []uint64{k}},
		op{kind: opRemove, keys: []uint64{k}},
		op{kind: opReply, from: from})
	return nil
}
