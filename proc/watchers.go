package proc

import (
	"sync"
	"sync/atomic"
)

// watcherShards is the number of shards of the watchers of a process.
const watcherShards = 8

// watchers are the watchers of a process, by ref, in shards made on the
// first: the callers of a busy server each add one and remove it on every
// call, and would all wait on one lock.
//
// A watcher is added under the lock of its shard, the process not dead,
// and the dying process takes them all once dead: one is either told of
// the death, or refused. The lock of a process, if held, is taken first.
type watchers struct {
	shards atomic.Pointer[[watcherShards]watcherShard]
}

type watcherShard struct {
	mu sync.Mutex
	m  map[Ref]watcher
	_  [48]byte // keep shards on separate cache lines
}

// add adds w under ref, unless dead is set, and reports whether it did.
func (ws *watchers) add(ref Ref, w watcher, dead *atomic.Bool) bool {
	s := ws.shard(ref)
	s.mu.Lock()
	defer s.mu.Unlock()
	if dead.Load() {
		return false
	}
	if s.m == nil {
		s.m = make(map[Ref]watcher)
	}
	s.m[ref] = w
	return true
}

// shard returns the shard of ref, the shards made if none yet.
func (ws *watchers) shard(ref Ref) *watcherShard {
	shards := ws.shards.Load()
	if shards == nil {
		ws.shards.CompareAndSwap(nil, new([watcherShards]watcherShard))
		shards = ws.shards.Load()
	}
	return &shards[ref.id%watcherShards]
}

// del removes the watcher of ref, if any.
func (ws *watchers) del(ref Ref) {
	if shards := ws.shards.Load(); shards != nil {
		s := &shards[ref.id%watcherShards]
		s.mu.Lock()
		delete(s.m, ref)
		s.mu.Unlock()
	}
}

// each runs f on every watcher, under the lock of its shard; f may delete
// it.
func (ws *watchers) each(f func(m map[Ref]watcher, ref Ref, w watcher)) {
	shards := ws.shards.Load()
	if shards == nil {
		return
	}
	for i := range shards {
		s := &shards[i]
		s.mu.Lock()
		for ref, w := range s.m {
			f(s.m, ref, w)
		}
		s.mu.Unlock()
	}
}

// len returns the number of watchers.
func (ws *watchers) len() int {
	shards := ws.shards.Load()
	if shards == nil {
		return 0
	}
	n := 0
	for i := range shards {
		s := &shards[i]
		s.mu.Lock()
		n += len(s.m)
		s.mu.Unlock()
	}
	return n
}

// take removes all the watchers and returns them, by shard: of the
// process dying, once dead.
func (ws *watchers) take() (taken [watcherShards]map[Ref]watcher) {
	shards := ws.shards.Load()
	if shards == nil {
		return taken
	}
	for i := range shards {
		s := &shards[i]
		s.mu.Lock()
		taken[i], s.m = s.m, nil
		s.mu.Unlock()
	}
	return taken
}
