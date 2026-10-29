package kvs

import (
	"hash/maphash"
	"iter"
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
	root atomic.Pointer[bnode[K, V]]
	size atomic.Int64
	gen  uint64 // of the nodes a write may change in place, under mu
	_    [cacheLine]byte
}

// store makes t the tree of s, under its lock: its size first, so that a
// reader seeing an entry in the root sees it counted.
func (s *treeShard[K, V]) store(t btree[K, V]) {
	s.size.Store(int64(t.size))
	s.root.Store(t.root)
}

func newTreeIndex[K comparable, V any](h hasher, cmp func(a, b K) int, lockless bool, live *atomic.Int64) *treeIndex[K, V] {
	return &treeIndex[K, V]{hasher: h, cmp: cmp, lockless: lockless, shards: make([]treeShard[K, V], 1<<h.bits), live: live}
}

func (x *treeIndex[K, V]) shard(k K) *treeShard[K, V] {
	if x.bits == 0 {
		return &x.shards[0]
	}
	return &x.shards[x.hasher.shard(maphash.Comparable(x.seed, k))]
}

// loaded returns the tree of s as stored.
func (x *treeIndex[K, V]) loaded(s *treeShard[K, V]) btree[K, V] {
	return btree[K, V]{cmp: x.cmp, root: s.root.Load(), size: int(s.size.Load())}
}

// tree returns the tree of s to write in, under its lock, and its
// generation.
func (x *treeIndex[K, V]) tree(s *treeShard[K, V]) (btree[K, V], uint64) {
	if x.lockless {
		s.gen++ // the root read without lock is not to change
	}
	return x.loaded(s), s.gen
}

func (x *treeIndex[K, V]) get(k K) (V, uint64, bool) {
	s := x.shard(k)
	if !x.lockless {
		s.mu.RLock()
	}
	l, ok := (&btree[K, V]{cmp: x.cmp, root: s.root.Load()}).get(k)
	var e leaf[K, V]
	if ok {
		e = *l
	}
	if !x.lockless {
		s.mu.RUnlock()
	}
	return e.Val, e.Meta.rev, ok
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
		if !match(cur.Val, cur.Meta.rev, ok) {
			s.mu.Unlock()
			return nil, ErrConflict
		}
	}
	seq, b, err := log.append(r)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	old, _ := t.set(gen, leaf[K, V]{Key: k, Val: v, Meta: meta{size: r.size(), rev: seq}})
	s.store(t)
	s.mu.Unlock()
	x.live.Add(r.size() - old.Meta.size)
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
	if match != nil && !match(cur.Val, cur.Meta.rev, ok) {
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
	s.store(t)
	s.mu.Unlock()
	x.live.Add(-cur.Meta.size)
	return b, nil
}

func (x *treeIndex[K, V]) len() int {
	n := 0
	for i := range x.shards {
		n += int(x.shards[i].size.Load())
	}
	return n
}

// frozen returns the root of shard i, to read without lock. With cut, it
// waits for the write under way, for the writes the log holds to be in
// the root.
func (x *treeIndex[K, V]) frozen(i int, cut bool) *bnode[K, V] {
	s := &x.shards[i]
	if x.lockless && !cut {
		return s.root.Load()
	}
	s.mu.Lock()
	if !x.lockless {
		s.gen++
	}
	n := s.root.Load()
	s.mu.Unlock()
	return n
}

func (x *treeIndex[K, V]) byShard(cut bool) iter.Seq[entries[K, V]] {
	return func(yield func(entries[K, V]) bool) {
		for i := range x.shards {
			if !yield(x.inOrder(x.frozen(i, cut))) {
				return
			}
		}
	}
}

// inOrder ranges over the entries under root, in order.
func (x *treeIndex[K, V]) inOrder(root *bnode[K, V]) entries[K, V] {
	return func(yield func(*leaf[K, V]) bool) {
		c := cursor[K, V]{cmp: x.cmp}
		for c.seek(root, nil); c.cur != nil && yield(c.cur); c.next() {
		}
	}
}

// ascend yields the entries from lo, if not nil, up to hi, if not nil, in
// order, merging those of the shards, each frozen as it was at a moment.
func (x *treeIndex[K, V]) ascend(lo, hi *K, yield func(*leaf[K, V]) bool) {
	x.ascendRoots(nil, lo, hi, yield)
}

// ascendRoots is ascend over the trees of roots, one a shard, or, if nil,
// over those of the shards, frozen.
//
//abc:hot BenchmarkOrdered
func (x *treeIndex[K, V]) ascendRoots(roots []*bnode[K, V], lo, hi *K, yield func(*leaf[K, V]) bool) {
	cs := x.cursors(roots, lo, hi)
	if len(cs) == 1 {
		// One shard, or one with entries in range: nothing to merge.
		for c := cs[0]; c.cur != nil && yield(c.cur); c.next() {
		}
		return
	}
	h := cursorHeap[K, V]{cmp: x.cmp, cs: cs}
	h.init()
	for len(h.cs) > 0 {
		c := h.cs[0]
		if !yield(c.cur) {
			return
		}
		if c.next(); c.cur == nil {
			h.cs[0] = h.cs[len(h.cs)-1]
			h.cs = h.cs[:len(h.cs)-1]
		}
		h.down(0)
	}
}

// cursors returns a cursor of each tree of roots, or, if nil, of each
// shard frozen, at its first entry from lo, if not nil, stopping before
// hi, if not nil; but those at no entry.
func (x *treeIndex[K, V]) cursors(roots []*bnode[K, V], lo, hi *K) []*cursor[K, V] {
	all := make([]cursor[K, V], len(x.shards))
	stacks := make([]frame[K, V], len(x.shards)*8) // deep enough for most
	cs := make([]*cursor[K, V], 0, len(x.shards))
	for i := range all {
		c := &all[i]
		c.cmp, c.stack = x.cmp, stacks[8*i:8*i:8*i+8]
		if hi != nil {
			c.hi, c.hasHi = *hi, true
		}
		root := x.rootOf(roots, i)
		if c.seek(root, lo); c.cur != nil {
			cs = append(cs, c)
		}
	}
	return cs
}

// rootOf returns the tree of shard i: roots[i], or, if roots is nil, that
// of the shard, frozen.
func (x *treeIndex[K, V]) rootOf(roots []*bnode[K, V], i int) *bnode[K, V] {
	if roots == nil {
		return x.frozen(i, false)
	}
	return roots[i]
}

// A cursorHeap is a heap of cursors, by the keys at them: the first at the
// least.
type cursorHeap[K comparable, V any] struct {
	cmp func(a, b K) int
	cs  []*cursor[K, V]
}

func (h *cursorHeap[K, V]) init() {
	for i := len(h.cs)/2 - 1; i >= 0; i-- {
		h.down(i)
	}
}

func (h *cursorHeap[K, V]) less(i, j int) bool {
	return h.cmp(h.cs[i].cur.Key, h.cs[j].cur.Key) < 0
}

// down moves the cursor i down the heap, to where it is not after its
// children.
func (h *cursorHeap[K, V]) down(i int) {
	for {
		m := i
		if l := 2*i + 1; l < len(h.cs) && h.less(l, m) {
			m = l
		}
		if r := 2*i + 2; r < len(h.cs) && h.less(r, m) {
			m = r
		}
		if m == i {
			return
		}
		h.cs[i], h.cs[m] = h.cs[m], h.cs[i]
		i = m
	}
}

func (x *treeIndex[K, V]) load(k K, v V, size int64, rev uint64) {
	s := x.shard(k)
	t := x.loaded(s) // no reader yet: changed in place
	old, _ := t.set(s.gen, leaf[K, V]{Key: k, Val: v, Meta: meta{size: size, rev: rev}})
	s.store(t)
	x.live.Add(size - old.Meta.size)
}

func (x *treeIndex[K, V]) unload(k K) {
	s := x.shard(k)
	t := x.loaded(s)
	if old, ok := t.del(s.gen, k); ok {
		s.store(t)
		x.live.Add(-old.Meta.size)
	}
}
