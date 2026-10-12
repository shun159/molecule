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
//
// A write with a match writes only if match, given the entry of its key,
// reports true, and fails with ErrConflict otherwise.
type index[K comparable, V any] interface {
	// get returns the value of k, and the seq of the write that set it.
	get(k K) (V, uint64, bool)
	// put sets k to v, written to log by r, unless appending r fails.
	put(log *wal, k K, v V, r rec, match matcher[V]) (*batch, error)
	// del deletes k, written to log by r. A key absent writes nothing.
	del(log *wal, k K, r rec, match matcher[V]) (*batch, error)
	len() int
	// eachShard hands each shard in turn to yield, as the entries it holds,
	// to range over without lock. With cut, they are those it holds
	// once locked, for a compaction.
	eachShard(cut bool, yield func(func(func(*leaf[K, V]) bool)) bool)
	// load sets or deletes entries on opening, without the log.
	load(k K, v V, size int64, rev uint64)
	unload(k K)
}

// matcher tells whether a write is to be made, given the entry of its
// key, its value and rev if ok: nil for any.
type matcher[V any] func(v V, rev uint64, ok bool) bool

func newIndex[K comparable, V any](read, write bool, shards int, cmp func(a, b K) int, live *atomic.Int64) index[K, V] {
	n := 1
	if write {
		n = shards
	}
	h := hasher{seed: maphash.MakeSeed()}
	for n > 1<<h.bits {
		h.bits++
	}
	if cmp != nil {
		x := &treeIndex[K, V]{hasher: h, cmp: cmp, lockless: read, shards: make([]treeShard[K, V], 1<<h.bits), live: live}
		for i := range x.shards {
			x.shards[i].root.Store(&troot[K, V]{})
		}
		return x
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
	rev  uint64
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

// treeIndex is a B-tree in each shard, in the order of the keys. Without
// ReadConcurrency, a write changes the tree in place, under the lock of
// the shard, and a range freezes it, taking a new generation, to read it
// without lock; with it, each write takes a new generation, and a read
// takes no lock.
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
