package supervisor

import (
	"errors"
	"slices"
	"time"

	"github.com/shun159/molecule"
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
	key  uint64
	spec ChildSpec
	pid  proc.PID // zero when not running
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
	from    molecule.From
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
	stopCalls []molecule.From
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

func (b sup) Init(self proc.PID, _ any) (state, []molecule.Effect, error) {
	s := state{self: self}
	effs := []molecule.Effect{molecule.TrapExit{On: true}}
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

func (b sup) Handle(s state, msg gen.Msg) (state, []molecule.Effect) {
	s = s.clone()
	var effs []molecule.Effect
	switch m := msg.(type) {
	case gen.ContinueMsg:
		if st, ok := m.Msg.(gen.Started); ok {
			effs = b.started(&s, st)
		}
	case molecule.CallMsg:
		effs = b.call(&s, m.From, m.Req)
	case gen.InfoMsg:
		effs = b.info(&s, m.Msg)
	}
	s, more := b.advance(s)
	return s, append(effs, more...)
}

// ParentExit stops the children, then the supervisor, with the reason of
// the parent, as a supervisor does.
func (b sup) ParentExit(s state, reason error) (state, []molecule.Effect) {
	s = s.clone()
	b.finish(&s, reason)
	return b.advance(s)
}

func (sup) Terminate(state, error) []molecule.Effect { return nil }

func (b sup) info(s *state, msg any) []molecule.Effect {
	switch m := msg.(type) {
	case proc.ExitMsg:
		if i := slices.Index(s.stopping, m.From); i >= 0 {
			s.stopping = slices.Delete(s.stopping, i, i+1)
			return molecule.Do(molecule.CancelTimer{Key: killKey{m.From}})
		}
		if s.byPID(m.From) < 0 {
			return nil // a child already stopped or replaced
		}
		if s.busy() {
			s.exits = append(s.exits, m)
			return nil
		}
		return b.exited(s, m)
	case kill:
		if slices.Contains(s.stopping, m.pid) {
			return molecule.Do(molecule.Exit{To: m.pid, Reason: proc.Kill})
		}
	case expired:
		s.restarts--
	case retry:
		if i := s.byKey(m.key); i >= 0 && s.children[i].pid.IsZero() && !s.final {
			return b.restart(s, i)
		}
	}
	return nil
}

func (s state) busy() bool { return s.waitStart || len(s.stopping) > 0 || len(s.ops) > 0 }

// exited handles the exit of a running child.
func (b sup) exited(s *state, m proc.ExitMsg) []molecule.Effect {
	i := s.byPID(m.From)
	if i < 0 {
		return nil
	}
	c := &s.children[i]
	effs := reportTerminated(s.self, b.childID(*c), m.From, c.spec.Restart, m.Reason)
	c.pid = proc.PID{}
	if !shouldRestart(c.spec.Restart, m.Reason) {
		if b.dynamic || c.spec.Restart == Temporary {
			s.children = slices.Delete(s.children, i, i+1)
		}
		return effs
	}
	return append(effs, b.restart(s, i)...)
}

// restart restarts the child at i, not running, and those the strategy
// involves, unless restarts are too many: then the supervisor gives up.
func (b sup) restart(s *state, i int) []molecule.Effect {
	if s.restarts+1 > b.max {
		effs := reportShutdown(s.self, ErrMaxIntensity)
		b.finish(s, ErrMaxIntensity)
		return effs
	}
	s.restarts++
	s.timerSeq++
	effs := molecule.Do(molecule.StartTimer{Key: expireKey{s.timerSeq}, After: b.period, Msg: expired{}})
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
func (b sup) started(s *state, st gen.Started) []molecule.Effect {
	if !s.waitStart || len(s.ops) == 0 {
		return nil
	}
	s.waitStart = false
	o := s.ops[0]
	s.ops = s.ops[1:]
	i := s.byKey(o.keys[0])
	c := &s.children[i]
	var effs []molecule.Effect
	switch {
	case errors.Is(st.Err, molecule.ErrIgnore):
		if b.dynamic {
			s.children = slices.Delete(s.children, i, i+1)
		}
		if o.call {
			effs = append(effs, molecule.Reply{To: o.from, Value: startResult{err: st.Err}})
		}
	case st.Err != nil:
		id := b.childID(*c)
		effs = append(effs, reportStartFailed(s.self, id, st.Err)...)
		switch {
		case o.initial:
			err := &StartError{ID: id, Reason: st.Err}
			s.final = true
			s.ops = append(b.stopAll(s), op{kind: opAck, err: err}, op{kind: opFinish, err: err})
		case o.call:
			s.children = slices.Delete(s.children, i, i+1)
			effs = append(effs, molecule.Reply{To: o.from, Value: startResult{err: st.Err}})
		default:
			// The children after it in the restart stay down until the
			// retry restarts them.
			for len(s.ops) > 0 && s.ops[0].kind == opStart && s.ops[0].restart {
				s.ops = s.ops[1:]
			}
			effs = append(effs, molecule.Send{To: s.self, Msg: retry{c.key}})
		}
	default:
		c.pid = st.PID
		effs = append(effs, reportStarted(s.self, b.childID(*c), st.PID)...)
		if o.call {
			effs = append(effs, molecule.Reply{To: o.from, Value: startResult{pid: st.PID}})
		}
	}
	return effs
}

// advance runs the operations queued, until one waits, then the exits that
// came meanwhile.
func (b sup) advance(s state) (state, []molecule.Effect) {
	var effs []molecule.Effect
	for !s.waitStart && len(s.stopping) == 0 {
		if len(s.ops) == 0 {
			if len(s.exits) == 0 || s.final {
				return s, effs
			}
			e := s.exits[0]
			s.exits = s.exits[1:]
			effs = append(effs, b.exited(&s, e)...)
			continue
		}
		o := s.ops[0]
		switch o.kind {
		case opStart:
			i := s.byKey(o.keys[0])
			switch {
			case i < 0 || !s.children[i].pid.IsZero():
				s.ops = s.ops[1:]
			case o.restart && s.children[i].spec.Restart == Temporary && !b.dynamic:
				// A temporary child stopped along with a sibling is not
				// restarted but forgotten, as in OTP.
				s.children = slices.Delete(s.children, i, i+1)
				s.ops = s.ops[1:]
			default:
				s.waitStart = true
				effs = append(effs, gen.StartChild{Child: s.children[i].spec.Start, Tag: startTag{o.keys[0]}})
			}
		case opStop:
			s.ops = s.ops[1:]
			for _, k := range o.keys {
				i := s.byKey(k)
				if i < 0 || s.children[i].pid.IsZero() {
					continue
				}
				c := &s.children[i]
				pid := c.pid
				c.pid = proc.PID{}
				s.stopping = append(s.stopping, pid)
				switch d := c.spec.shutdown(); d {
				case Brutal:
					effs = append(effs, molecule.Exit{To: pid, Reason: proc.Kill})
				case Infinity:
					effs = append(effs, molecule.Exit{To: pid, Reason: proc.Shutdown})
				default:
					effs = append(effs,
						molecule.Exit{To: pid, Reason: proc.Shutdown},
						molecule.StartTimer{Key: killKey{pid}, After: d, Msg: kill{pid}})
				}
			}
		case opRemove:
			if i := s.byKey(o.keys[0]); i >= 0 {
				s.children = slices.Delete(s.children, i, i+1)
			}
			s.ops = s.ops[1:]
		case opReply:
			effs = append(effs, molecule.Reply{To: o.from, Value: o.value})
			s.ops = s.ops[1:]
		case opAck:
			effs = append(effs, gen.Ack{Err: o.err})
			s.ops = s.ops[1:]
		case opFinish:
			for _, from := range s.stopCalls {
				effs = append(effs, molecule.Reply{To: from})
			}
			s.ops = nil
			return s, append(effs, molecule.Stop{Reason: o.err})
		}
	}
	return s, effs
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
		infos = append(infos, ChildInfo{ID: b.childID(c), PID: c.pid, Type: c.spec.Type, Restart: c.spec.Restart})
	}
	return infos
}

// call handles the calls of the API.
func (b sup) call(s *state, from molecule.From, req any) []molecule.Effect {
	reply := func(v any) []molecule.Effect { return molecule.Do(molecule.Reply{To: from, Value: v}) }
	switch r := req.(type) {
	case whichChildren:
		return reply(b.which(*s))
	case stopReq:
		s.stopCalls = append(s.stopCalls, from)
		b.finish(s, proc.Shutdown)
	case countChildren:
		return reply(len(s.children))
	case startChild:
		switch {
		case !b.dynamic:
			return reply(startResult{err: errors.New("supervisor: StartChild of a static supervisor")})
		case s.final:
			return reply(startResult{err: proc.Shutdown})
		case b.maxChildren > 0 && len(s.children) >= b.maxChildren:
			return reply(startResult{err: ErrMaxChildren})
		}
		s.nextKey++
		s.children = append(s.children, child{key: s.nextKey, spec: r.spec})
		s.ops = append(s.ops, op{kind: opStart, keys: []uint64{s.nextKey}, call: true, from: from})
	case terminateChild:
		i := s.byPID(r.pid)
		if i < 0 || !b.dynamic {
			return reply(ErrNotFound)
		}
		k := s.children[i].key
		s.ops = append(s.ops,
			op{kind: opStop, keys: []uint64{k}},
			op{kind: opRemove, keys: []uint64{k}},
			op{kind: opReply, from: from})
	}
	return nil
}
