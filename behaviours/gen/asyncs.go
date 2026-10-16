package gen

import "github.com/shun159/molecule/actor"

// asyncDone is the outcome of an Async.
type asyncDone struct {
	key   any
	gen   uint64
	value any
	err   error
}

// async is an Async started: running, or, with no cancel, waiting.
type async struct {
	cancel func()
	gen    uint64
}

// queued is an Async waiting to run; it runs only if still in asyncs
// under its gen, not cancelled or replaced meanwhile.
type queued struct {
	e   actor.Async
	gen uint64
}

// asyncs are the Asyncs of a behaviour, by key: running, no more than max
// at once if max > 0, or waiting their turn, in order. One cancelled keeps
// its place until its Run returns, which Go cannot make it do.
type asyncs struct {
	env      Env
	byKey    map[any]async
	gen      uint64
	max      int
	running  int             // cancelled ones too, until they return
	waiting  []queued        // in order
	draining map[uint64]bool // gens of those cancelled, still running
}

func newAsyncs(env Env, max int) asyncs {
	return asyncs{env: env, byKey: make(map[any]async), max: max, draining: make(map[uint64]bool)}
}

// start runs e, or, with max running, has it wait its turn after those
// waiting already. One under its key is cancelled first.
func (a *asyncs) start(e actor.Async) {
	a.cancel(e.Key)
	a.runWaiting()
	a.gen++
	if a.full() {
		a.byKey[e.Key] = async{gen: a.gen}
		a.waiting = append(a.waiting, queued{e, a.gen})
		return
	}
	a.run(e, a.gen)
}

func (a *asyncs) run(e actor.Async, gen uint64) {
	key := e.Key
	cancel := a.env.Async(e.Run, func(v any, err error) any {
		return asyncDone{key: key, gen: gen, value: v, err: err}
	})
	a.byKey[key] = async{cancel: cancel, gen: gen}
	a.running++
}

// full reports whether max are running: one more waits its turn.
func (a *asyncs) full() bool { return a.max > 0 && a.running >= a.max }

// runWaiting runs those waiting, in order, while under max.
func (a *asyncs) runWaiting() {
	for len(a.waiting) > 0 && !a.full() {
		q := a.waiting[0]
		a.waiting[0] = queued{}
		a.waiting = a.waiting[1:]
		if len(a.waiting) == 0 {
			a.waiting = nil
		}
		if cur, ok := a.byKey[q.e.Key]; ok && cur.gen == q.gen {
			a.run(q.e, q.gen)
		}
	}
}

// cancel cancels the async under key, running or waiting.
func (a *asyncs) cancel(key any) {
	cur, ok := a.byKey[key]
	if !ok {
		return
	}
	if cur.cancel != nil {
		cur.cancel()
		a.draining[cur.gen] = true
	}
	delete(a.byKey, key)
}

// done frees the place of the async of m, now returned, and runs the next
// waiting; it returns the outcome for the behaviour, unless the async was
// cancelled or replaced.
func (a *asyncs) done(m asyncDone) (Msg, bool) {
	cur, ok := a.byKey[m.key]
	if !ok || cur.gen != m.gen {
		// Cancelled, or replaced: its outcome is not for the behaviour,
		// but its place is free now it has returned.
		if a.draining[m.gen] {
			delete(a.draining, m.gen)
			a.running--
			a.runWaiting()
		}
		return nil, false
	}
	delete(a.byKey, m.key)
	a.running--
	a.runWaiting()
	return InfoMsg{Msg: actor.AsyncResult{Key: m.key, Value: m.value, Err: m.err}}, true
}

// abort cancels them all, those waiting never to run.
func (a *asyncs) abort() {
	for key := range a.byKey {
		a.cancel(key)
	}
	a.waiting = nil
}
