package kvs

import (
	"hash/maphash"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/shun159/molecule/internal/champ"
)

// hamtIndex is a persistent trie in each shard, its root replaced by each
// write, under a Mutex, and read without lock.
type hamtIndex[K comparable, V any] struct {
	hasher
	shards []hamtShard[K, V]
	live   *atomic.Int64
}

// A write sets size before root: a reader seeing an entry in root sees it
// counted.
type hamtShard[K comparable, V any] struct {
	mu   sync.Mutex
	root atomic.Pointer[hnode[K, V]]
	size atomic.Int64
	_    [cacheLine]byte
}

// hnode is a node of the trie of a shard.
type hnode[K comparable, V any] = champ.Node[K, V, meta]

func newHamtIndex[K comparable, V any](h hasher, live *atomic.Int64) *hamtIndex[K, V] {
	x := &hamtIndex[K, V]{hasher: h, shards: make([]hamtShard[K, V], 1<<h.bits), live: live}
	for i := range x.shards {
		x.shards[i].root.Store(&hnode[K, V]{})
	}
	return x
}

func (x *hamtIndex[K, V]) get(k K) (V, uint64, bool) {
	h := maphash.Comparable(x.seed, k)
	l, ok := x.shards[x.hasher.shard(h)].root.Load().Get(h, k)
	if !ok {
		var zero V
		return zero, 0, false
	}
	return l.Val, l.Meta.rev, true
}

//abc:hot BenchmarkPut,BenchmarkMixed
func (x *hamtIndex[K, V]) put(log *wal, k K, v V, r rec, match matcher[V]) (*batch, error) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	s.mu.Lock()
	cur := s.root.Load()
	if match != nil && !matches(cur, h, k, match) {
		s.mu.Unlock()
		return nil, ErrConflict
	}
	seq, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	n, old, replaced := cur.Set(leaf[K, V]{Hash: h, Key: k, Val: v, Meta: meta{size: r.size(), rev: seq}})
	if !replaced {
		s.size.Add(1)
	}
	s.root.Store(n)
	s.mu.Unlock()
	x.live.Add(r.size() - old.Meta.size)
	return b, nil
}

// matches reports whether the entry of k, of hash h, in n, or its lack,
// is what match asks for.
func matches[K comparable, V any](n *hnode[K, V], h uint64, k K, match matcher[V]) bool {
	var l leaf[K, V]
	p, ok := n.Get(h, k)
	if ok {
		l = *p
	}
	return match(l.Val, l.Meta.rev, ok)
}

func (x *hamtIndex[K, V]) del(log *wal, k K, r rec, match matcher[V]) (*batch, error) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	s.mu.Lock()
	cur := s.root.Load()
	n, old, ok := cur.Delete(h, k)
	if match != nil && !match(old.Val, old.Meta.rev, ok) {
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
	s.size.Add(-1)
	s.root.Store(n)
	s.mu.Unlock()
	x.live.Add(-old.Meta.size)
	return b, nil
}

func (x *hamtIndex[K, V]) len() int {
	n := 0
	for i := range x.shards {
		n += int(x.shards[i].size.Load())
	}
	return n
}

func (x *hamtIndex[K, V]) byShard(cut bool) iter.Seq[entries[K, V]] {
	return func(yield func(entries[K, V]) bool) {
		for i := range x.shards {
			root := x.root(i, cut)
			if !yield(func(f func(*leaf[K, V]) bool) { root.Each(f) }) {
				return
			}
		}
	}
}

// root returns the root of shard i, to read without lock. With cut, it
// waits for the write under way, for the writes the log holds to be in it.
func (x *hamtIndex[K, V]) root(i int, cut bool) *hnode[K, V] {
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
	n := s.root.Load() // no reader yet: changed in place
	old, replaced := n.SetInPlace(leaf[K, V]{Hash: h, Key: k, Val: v, Meta: meta{size: size, rev: rev}})
	if !replaced {
		s.size.Add(1)
	}
	x.live.Add(size - old.Meta.size)
}

func (x *hamtIndex[K, V]) unload(k K) {
	h := maphash.Comparable(x.seed, k)
	s := &x.shards[x.hasher.shard(h)]
	if n, old, ok := s.root.Load().Delete(h, k); ok {
		s.size.Add(-1)
		s.root.Store(n)
		x.live.Add(-old.Meta.size)
	}
}
