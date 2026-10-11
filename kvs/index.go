package kvs

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// index holds the entries of a table in memory. A write takes the lock of
// the shard of its key, appends its record to the log, then changes the
// entries, before letting go: the order of the writes to a key is the
// same in memory and in the log, and a compaction locking a shard after
// cutting the log finds every write the log holds up to the cut.
type index[K comparable, V any] interface {
	get(k K) (V, bool)
	// put sets k to v, written to log by r, unless appending r fails.
	put(log *wal, k K, v V, r rec) (*batch, error)
	// del deletes k, written to log by r. A key absent writes nothing.
	del(log *wal, k K, r rec) (*batch, error)
	len() int
	// eachShard hands each shard in turn to yield, as the entries it holds,
	// to range over without lock. With cut, they are those it holds
	// once locked, for a compaction.
	eachShard(cut bool, yield func(func(func(*leaf[K, V]) bool)) bool)
	// load sets or deletes entries on opening, without the log.
	load(k K, v V, size int64)
	unload(k K)
}

func newIndex[K comparable, V any](read, write bool, shards int, live *atomic.Int64) index[K, V] {
	n := 1
	if write {
		n = shards
	}
	h := hasher{seed: maphash.MakeSeed()}
	for n > 1<<h.bits {
		h.bits++
	}
	if read {
		x := &hamtIndex[K, V]{hasher: h, shards: make([]hamtShard[K, V], 1<<h.bits), live: live}
		for i := range x.shards {
			x.shards[i].root.Store(&hroot[K, V]{n: &hnode[K, V]{}})
		}
		return x
	}
	x := &mapIndex[K, V]{hasher: h, shards: make([]mapShard[K, V], 1<<h.bits), live: live}
	for i := range x.shards {
		x.shards[i].m = make(map[K]entry[V])
	}
	return x
}

// hasher hashes keys, and picks the shard of a hash by its top bits, the
// trie using the others first.
type hasher struct {
	seed maphash.Seed
	bits uint
}

func (h hasher) shard(hash uint64) uint64 {
	if h.bits == 0 {
		return 0
	}
	return hash >> (64 - h.bits)
}

// Shards are as large as a cache line or more, so that two are not in one.
const cacheLine = 64

// mapIndex is a map in each shard, under a RWMutex.
type mapIndex[K comparable, V any] struct {
	hasher
	shards []mapShard[K, V]
	live   *atomic.Int64
}

type mapShard[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]entry[V]
	_  [cacheLine]byte
}

type entry[V any] struct {
	val  V
	size int64
}

func (x *mapIndex[K, V]) shard(k K) *mapShard[K, V] {
	if x.bits == 0 {
		return &x.shards[0]
	}
	return &x.shards[x.hasher.shard(maphash.Comparable(x.seed, k))]
}

func (x *mapIndex[K, V]) get(k K) (V, bool) {
	s := x.shard(k)
	s.mu.RLock()
	e, ok := s.m[k]
	s.mu.RUnlock()
	return e.val, ok
}

func (x *mapIndex[K, V]) put(log *wal, k K, v V, r rec) (*batch, error) {
	s := x.shard(k)
	s.mu.Lock()
	_, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	old := s.m[k]
	s.m[k] = entry[V]{v, r.size()}
	s.mu.Unlock()
	x.live.Add(r.size() - old.size)
	return b, nil
}

func (x *mapIndex[K, V]) del(log *wal, k K, r rec) (*batch, error) {
	s := x.shard(k)
	s.mu.Lock()
	old, ok := s.m[k]
	if !ok {
		s.mu.Unlock()
		return log.barrier()
	}
	_, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	delete(s.m, k)
	s.mu.Unlock()
	x.live.Add(-old.size)
	return b, nil
}

func (x *mapIndex[K, V]) len() int {
	n := 0
	for i := range x.shards {
		s := &x.shards[i]
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}

func (x *mapIndex[K, V]) eachShard(_ bool, yield func(func(func(*leaf[K, V]) bool)) bool) {
	for i := range x.shards {
		s := &x.shards[i]
		s.mu.RLock()
		ls := make([]leaf[K, V], 0, len(s.m))
		for k, e := range s.m {
			ls = append(ls, leaf[K, V]{key: k, val: e.val, size: e.size})
		}
		s.mu.RUnlock()
		each := func(f func(*leaf[K, V]) bool) {
			for i := range ls {
				if !f(&ls[i]) {
					return
				}
			}
		}
		if !yield(each) {
			return
		}
	}
}

func (x *mapIndex[K, V]) load(k K, v V, size int64) {
	s := x.shard(k)
	old := s.m[k]
	s.m[k] = entry[V]{v, size}
	x.live.Add(size - old.size)
}

func (x *mapIndex[K, V]) unload(k K) {
	s := x.shard(k)
	if old, ok := s.m[k]; ok {
		delete(s.m, k)
		x.live.Add(-old.size)
	}
}

// hamtIndex is a persistent trie in each shard, its root replaced by each
// write, under a Mutex, and read without lock.
type hamtIndex[K comparable, V any] struct {
	hasher
	shards []hamtShard[K, V]
	live   *atomic.Int64
}

type hamtShard[K comparable, V any] struct {
	mu   sync.Mutex
	root atomic.Pointer[hroot[K, V]]
	_    [cacheLine]byte
}

type hroot[K comparable, V any] struct {
	n    *hnode[K, V]
	size int
}

func (x *hamtIndex[K, V]) get(k K) (V, bool) {
	h := maphash.Comparable(x.seed, k)
	l, ok := x.shards[x.hasher.shard(h)].root.Load().n.get(h, k)
	if !ok {
		var zero V
		return zero, false
	}
	return l.val, true
}

func (x *hamtIndex[K, V]) put(log *wal, k K, v V, r rec) (*batch, error) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	s.mu.Lock()
	_, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	cur := s.root.Load()
	n, old, replaced := cur.n.set(0, leaf[K, V]{hash: h, key: k, val: v, size: r.size()})
	size := cur.size
	if !replaced {
		size++
	}
	s.root.Store(&hroot[K, V]{n: n, size: size})
	s.mu.Unlock()
	x.live.Add(r.size() - old.size)
	return b, nil
}

func (x *hamtIndex[K, V]) del(log *wal, k K, r rec) (*batch, error) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	s.mu.Lock()
	cur := s.root.Load()
	n, old, ok := cur.n.del(0, h, k)
	if !ok {
		s.mu.Unlock()
		return log.barrier()
	}
	_, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.root.Store(&hroot[K, V]{n: n, size: cur.size - 1})
	s.mu.Unlock()
	x.live.Add(-old.size)
	return b, nil
}

func (x *hamtIndex[K, V]) len() int {
	n := 0
	for i := range x.shards {
		n += x.shards[i].root.Load().size
	}
	return n
}

func (x *hamtIndex[K, V]) eachShard(cut bool, yield func(func(func(*leaf[K, V]) bool)) bool) {
	for i := range x.shards {
		s := &x.shards[i]
		if cut {
			s.mu.Lock() // for the writes the log holds to be in the root
		}
		root := s.root.Load()
		if cut {
			s.mu.Unlock()
		}
		if !yield(func(f func(*leaf[K, V]) bool) { root.n.each(f) }) {
			return
		}
	}
}

func (x *hamtIndex[K, V]) load(k K, v V, size int64) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	r := s.root.Load() // no reader yet: changed in place
	old, replaced := r.n.setInPlace(0, leaf[K, V]{hash: h, key: k, val: v, size: size})
	if !replaced {
		r.size++
	}
	x.live.Add(size - old.size)
}

func (x *hamtIndex[K, V]) unload(k K) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	cur := s.root.Load()
	if n, old, ok := cur.n.del(0, h, k); ok {
		s.root.Store(&hroot[K, V]{n: n, size: cur.size - 1})
		x.live.Add(-old.size)
	}
}
