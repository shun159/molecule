package kvs

import (
	"hash/maphash"
	"iter"
	"sync"
	"sync/atomic"
)

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

func newHamtIndex[K comparable, V any](h hasher, live *atomic.Int64) *hamtIndex[K, V] {
	x := &hamtIndex[K, V]{hasher: h, shards: make([]hamtShard[K, V], 1<<h.bits), live: live}
	for i := range x.shards {
		x.shards[i].root.Store(&hroot[K, V]{n: &hnode[K, V]{}})
	}
	return x
}

func (x *hamtIndex[K, V]) get(k K) (V, uint64, bool) {
	h := maphash.Comparable(x.seed, k)
	l, ok := x.shards[x.hasher.shard(h)].root.Load().n.get(h, k)
	if !ok {
		var zero V
		return zero, 0, false
	}
	return l.val, l.rev, true
}

func (x *hamtIndex[K, V]) put(log *wal, k K, v V, r rec, match matcher[V]) (*batch, error) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	s.mu.Lock()
	cur := s.root.Load()
	if match != nil {
		var l leaf[K, V]
		p, ok := cur.n.get(h, k)
		if ok {
			l = *p
		}
		if !match(l.val, l.rev, ok) {
			s.mu.Unlock()
			return nil, ErrConflict
		}
	}
	seq, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	n, old, replaced := cur.n.set(0, leaf[K, V]{hash: h, key: k, val: v, size: r.size(), rev: seq})
	size := cur.size
	if !replaced {
		size++
	}
	s.root.Store(&hroot[K, V]{n: n, size: size})
	s.mu.Unlock()
	x.live.Add(r.size() - old.size)
	return b, nil
}

func (x *hamtIndex[K, V]) del(log *wal, k K, r rec, match matcher[V]) (*batch, error) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	s.mu.Lock()
	cur := s.root.Load()
	n, old, ok := cur.n.del(0, h, k)
	if match != nil && !match(old.val, old.rev, ok) {
		s.mu.Unlock()
		return nil, ErrConflict
	}
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

func (x *hamtIndex[K, V]) byShard(cut bool) iter.Seq[entries[K, V]] {
	return func(yield func(entries[K, V]) bool) {
		for i := range x.shards {
			root := x.root(i, cut)
			if !yield(func(f func(*leaf[K, V]) bool) { root.n.each(f) }) {
				return
			}
		}
	}
}

// root returns the root of shard i, to read without lock. With cut, it
// waits for the write under way, for the writes the log holds to be in it.
func (x *hamtIndex[K, V]) root(i int, cut bool) *hroot[K, V] {
	s := &x.shards[i]
	if cut {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	return s.root.Load()
}

func (x *hamtIndex[K, V]) load(k K, v V, size int64, rev uint64) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	r := s.root.Load() // no reader yet: changed in place
	old, replaced := r.n.setInPlace(0, leaf[K, V]{hash: h, key: k, val: v, size: size, rev: rev})
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
