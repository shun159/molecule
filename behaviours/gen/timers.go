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

func (r *runner[S]) startTimer(e actor.StartTimer) {
	r.cancelTimer(e.Key)
	r.timerGen++
	after := e.After
	if !e.At.IsZero() {
		after = max(0, e.At.Sub(r.env.Now()))
	}
	cancel := r.env.SendAfter(after, timeout{key: e.Key, gen: r.timerGen})
	r.timers[e.Key] = timer{cancel: cancel, gen: r.timerGen, msg: e.Msg}
}

func (r *runner[S]) cancelTimer(key any) {
	if t, ok := r.timers[key]; ok {
		t.cancel()
		delete(r.timers, key)
	}
}
