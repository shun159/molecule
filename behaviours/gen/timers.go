package gen

import "github.com/shun159/molecule/actor"

// timeout is the firing of a timer, of its gen.
type timeout struct {
	key any
	gen uint64
}

type timer struct {
	cancel func()
	gen    uint64
	msg    any
}

// timers are the timers of a behaviour, by key. One fires only under the
// gen it started with: a timeout of one cancelled or started again since
// is dropped.
type timers struct {
	env   Env
	byKey map[any]timer
	gen   uint64
}

func newTimers(env Env) timers { return timers{env: env, byKey: make(map[any]timer)} }

// start starts the timer of e, cancelling one under its key.
func (t *timers) start(e actor.StartTimer) {
	t.cancel(e.Key)
	t.gen++
	after := e.After
	if !e.At.IsZero() {
		after = max(0, e.At.Sub(t.env.Now()))
	}
	cancel := t.env.SendAfter(after, timeout{key: e.Key, gen: t.gen})
	t.byKey[e.Key] = timer{cancel: cancel, gen: t.gen, msg: e.Msg}
}

func (t *timers) cancel(key any) {
	if tm, ok := t.byKey[key]; ok {
		tm.cancel()
		delete(t.byKey, key)
	}
}

// fired returns the Msg of the timer of m, unless it is cancelled or
// started again since.
func (t *timers) fired(m timeout) (Msg, bool) {
	tm, ok := t.byKey[m.key]
	if !ok || tm.gen != m.gen {
		return nil, false
	}
	delete(t.byKey, m.key)
	return InfoMsg{Msg: tm.msg}, true
}

func (t *timers) abort() {
	for key := range t.byKey {
		t.cancel(key)
	}
}
