package kvs

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

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
	rev  uint64
}

func newMapIndex[K comparable, V any](h hasher, live *atomic.Int64) *mapIndex[K, V] {
	x := &mapIndex[K, V]{hasher: h, shards: make([]mapShard[K, V], 1<<h.bits), live: live}
	for i := range x.shards {
		x.shards[i].m = make(map[K]entry[V])
	}
	return x
}

func (x *mapIndex[K, V]) shard(k K) *mapShard[K, V] {
	if x.bits == 0 {
		return &x.shards[0]
	}
	return &x.shards[x.hasher.shard(maphash.Comparable(x.seed, k))]
}

func (x *mapIndex[K, V]) get(k K) (V, uint64, bool) {
	s := x.shard(k)
	s.mu.RLock()
	e, ok := s.m[k]
	s.mu.RUnlock()
	return e.val, e.rev, ok
}

func (x *mapIndex[K, V]) put(log *wal, k K, v V, r rec, match matcher[V]) (*batch, error) {
	s := x.shard(k)
	s.mu.Lock()
	old, ok := s.m[k]
	if match != nil && !match(old.val, old.rev, ok) {
		s.mu.Unlock()
		return nil, ErrConflict
	}
	seq, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.m[k] = entry[V]{v, r.size(), seq}
	s.mu.Unlock()
	x.live.Add(r.size() - old.size)
	return b, nil
}

func (x *mapIndex[K, V]) del(log *wal, k K, r rec, match matcher[V]) (*batch, error) {
	s := x.shard(k)
	s.mu.Lock()
	old, ok := s.m[k]
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
			ls = append(ls, leaf[K, V]{key: k, val: e.val, size: e.size, rev: e.rev})
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

func (x *mapIndex[K, V]) load(k K, v V, size int64, rev uint64) {
	s := x.shard(k)
	old := s.m[k]
	s.m[k] = entry[V]{v, size, rev}
	x.live.Add(size - old.size)
}

func (x *mapIndex[K, V]) unload(k K) {
	s := x.shard(k)
	if old, ok := s.m[k]; ok {
		delete(s.m, k)
		x.live.Add(-old.size)
	}
}
