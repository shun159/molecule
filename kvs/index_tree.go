package kvs

import (
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// treeIndex is a B-tree in each shard, in the order of the keys, for an
// OrderedSet. Without ReadConcurrency, a write changes the tree in place,
// under the lock of the shard, and a range freezes it, taking a new
// generation, to read it without lock; with it, each write takes a new
// generation, and a read takes no lock.
type treeIndex[K comparable, V any] struct {
	hasher
	cmp      func(a, b K) int
	lockless bool
	shards   []treeShard[K, V]
	live     *atomic.Int64
}

type treeShard[K comparable, V any] struct {
	mu   sync.RWMutex
	root atomic.Pointer[troot[K, V]]
	gen  uint64 // of the nodes a write may change in place, under mu
	_    [cacheLine]byte
}

type troot[K comparable, V any] struct {
	n    *bnode[K, V]
	size int
}

func newTreeIndex[K comparable, V any](h hasher, cmp func(a, b K) int, lockless bool, live *atomic.Int64) *treeIndex[K, V] {
	x := &treeIndex[K, V]{hasher: h, cmp: cmp, lockless: lockless, shards: make([]treeShard[K, V], 1<<h.bits), live: live}
	for i := range x.shards {
		x.shards[i].root.Store(&troot[K, V]{})
	}
	return x
}

func (x *treeIndex[K, V]) shard(k K) *treeShard[K, V] {
	if x.bits == 0 {
		return &x.shards[0]
	}
	return &x.shards[x.hasher.shard(maphash.Comparable(x.seed, k))]
}

// tree returns the tree of s to write in, under its lock, and its
// generation.
func (x *treeIndex[K, V]) tree(s *treeShard[K, V]) (btree[K, V], uint64) {
	if x.lockless {
		s.gen++ // the root read without lock is not to change
	}
	r := s.root.Load()
	return btree[K, V]{cmp: x.cmp, root: r.n, size: r.size}, s.gen
}

func (x *treeIndex[K, V]) get(k K) (V, uint64, bool) {
	s := x.shard(k)
	if !x.lockless {
		s.mu.RLock()
	}
	r := s.root.Load()
	l, ok := (&btree[K, V]{cmp: x.cmp, root: r.n}).get(k)
	var e leaf[K, V]
	if ok {
		e = *l
	}
	if !x.lockless {
		s.mu.RUnlock()
	}
	return e.val, e.rev, ok
}

func (x *treeIndex[K, V]) put(log *wal, k K, v V, r rec, match matcher[V]) (*batch, error) {
	s := x.shard(k)
	s.mu.Lock()
	t, gen := x.tree(s)
	if match != nil {
		var cur leaf[K, V]
		p, ok := t.get(k)
		if ok {
			cur = *p
		}
		if !match(cur.val, cur.rev, ok) {
			s.mu.Unlock()
			return nil, ErrConflict
		}
	}
	seq, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	old, _ := t.set(gen, leaf[K, V]{key: k, val: v, size: r.size(), rev: seq})
	s.root.Store(&troot[K, V]{t.root, t.size})
	s.mu.Unlock()
	x.live.Add(r.size() - old.size)
	return b, nil
}

func (x *treeIndex[K, V]) del(log *wal, k K, r rec, match matcher[V]) (*batch, error) {
	s := x.shard(k)
	s.mu.Lock()
	t, gen := x.tree(s)
	var cur leaf[K, V]
	p, ok := t.get(k)
	if ok {
		cur = *p
	}
	if match != nil && !match(cur.val, cur.rev, ok) {
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
	t.del(gen, k)
	s.root.Store(&troot[K, V]{t.root, t.size})
	s.mu.Unlock()
	x.live.Add(-cur.size)
	return b, nil
}

func (x *treeIndex[K, V]) len() int {
	n := 0
	for i := range x.shards {
		n += x.shards[i].root.Load().size
	}
	return n
}

// frozen returns the root of shard i, to read without lock. With cut, it
// waits for the write under way, for the writes the log holds to be in
// the root.
func (x *treeIndex[K, V]) frozen(i int, cut bool) *bnode[K, V] {
	s := &x.shards[i]
	if x.lockless && !cut {
		return s.root.Load().n
	}
	s.mu.Lock()
	if !x.lockless {
		s.gen++
	}
	n := s.root.Load().n
	s.mu.Unlock()
	return n
}

func (x *treeIndex[K, V]) eachShard(cut bool, yield func(func(func(*leaf[K, V]) bool)) bool) {
	for i := range x.shards {
		root := x.frozen(i, cut)
		each := func(f func(*leaf[K, V]) bool) {
			c := cursor[K, V]{cmp: x.cmp}
			for c.seek(root, nil); c.cur != nil && f(c.cur); c.next() {
			}
		}
		if !yield(each) {
			return
		}
	}
}

// ascend yields the entries from lo, if not nil, up to hi, if not nil, in
// order, merging those of the shards, each frozen as it was at a moment.
func (x *treeIndex[K, V]) ascend(lo, hi *K, yield func(*leaf[K, V]) bool) {
	all := make([]cursor[K, V], len(x.shards))
	stacks := make([]frame[K, V], len(x.shards)*8) // deep enough for most
	cs := make([]*cursor[K, V], 0, len(x.shards))
	for i := range all {
		c := &all[i]
		c.cmp, c.stack = x.cmp, stacks[8*i:8*i:8*i+8]
		if hi != nil {
			c.hi, c.hasHi = *hi, true
		}
		if c.seek(x.frozen(i, false), lo); c.cur != nil {
			cs = append(cs, c)
		}
	}
	// A heap of the cursors, by the keys at them.
	less := func(i, j int) bool { return x.cmp(cs[i].cur.key, cs[j].cur.key) < 0 }
	down := func(i int) {
		for {
			m := i
			if l := 2*i + 1; l < len(cs) && less(l, m) {
				m = l
			}
			if r := 2*i + 2; r < len(cs) && less(r, m) {
				m = r
			}
			if m == i {
				return
			}
			cs[i], cs[m] = cs[m], cs[i]
			i = m
		}
	}
	for i := len(cs)/2 - 1; i >= 0; i-- {
		down(i)
	}
	for len(cs) > 0 {
		c := cs[0]
		if !yield(c.cur) {
			return
		}
		if c.next(); c.cur == nil {
			cs[0] = cs[len(cs)-1]
			cs = cs[:len(cs)-1]
		}
		down(0)
	}
}

func (x *treeIndex[K, V]) load(k K, v V, size int64, rev uint64) {
	s := x.shard(k)
	r := s.root.Load() // no reader yet: changed in place
	t := btree[K, V]{cmp: x.cmp, root: r.n, size: r.size}
	old, _ := t.set(s.gen, leaf[K, V]{key: k, val: v, size: size, rev: rev})
	s.root.Store(&troot[K, V]{t.root, t.size})
	x.live.Add(size - old.size)
}

func (x *treeIndex[K, V]) unload(k K) {
	s := x.shard(k)
	r := s.root.Load()
	t := btree[K, V]{cmp: x.cmp, root: r.n, size: r.size}
	if old, ok := t.del(s.gen, k); ok {
		s.root.Store(&troot[K, V]{t.root, t.size})
		x.live.Add(-old.size)
	}
}
