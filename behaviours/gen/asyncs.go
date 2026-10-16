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

// startAsync runs e, or, with maxAsync running, has it wait its turn
// after those waiting already.
func (r *runner[S]) startAsync(e actor.Async) {
	r.cancelAsync(e.Key)
	r.runWaiting()
	r.asyncGen++
	if r.maxAsync > 0 && r.running >= r.maxAsync {
		r.asyncs[e.Key] = async{gen: r.asyncGen}
		r.waiting = append(r.waiting, queued{e, r.asyncGen})
		return
	}
	r.runAsync(e, r.asyncGen)
}

func (r *runner[S]) runAsync(e actor.Async, gen uint64) {
	key := e.Key
	cancel := r.env.Async(e.Run, func(v any, err error) any {
		return asyncDone{key: key, gen: gen, value: v, err: err}
	})
	r.asyncs[key] = async{cancel: cancel, gen: gen}
	r.running++
}

// runWaiting runs the asyncs waiting, in order, while under maxAsync.
func (r *runner[S]) runWaiting() {
	for len(r.waiting) > 0 && (r.maxAsync <= 0 || r.running < r.maxAsync) {
		q := r.waiting[0]
		r.waiting[0] = queued{}
		r.waiting = r.waiting[1:]
		if len(r.waiting) == 0 {
			r.waiting = nil
		}
		if a, ok := r.asyncs[q.e.Key]; ok && a.gen == q.gen {
			r.runAsync(q.e, q.gen)
		}
	}
}

// cancelAsync cancels the async under key, running or waiting. One
// running keeps its place until its Run returns, which Go cannot make it
// do: so no more than maxAsync goroutines run at once.
func (r *runner[S]) cancelAsync(key any) {
	a, ok := r.asyncs[key]
	if !ok {
		return
	}
	if a.cancel != nil {
		a.cancel()
		r.draining[a.gen] = true
	}
	delete(r.asyncs, key)
}
