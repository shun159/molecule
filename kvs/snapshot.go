package kvs

import (
	"hash/maphash"
	"iter"
	"maps"
)

// Snapshot is a table as it was at a moment: every write that returned
// before it was taken is in it, none that began after; it reads without
// lock, as long as it is kept, the table written meanwhile. It is made by
// Table.Snapshot, and is a value, to keep in the state of a behaviour.
//
// Of an OrderedSet, or a table with ReadConcurrency, a snapshot costs
// little to take, whatever the size of the table: it keeps the trees, or
// the tries, of the moment, which writes after it copy rather than
// change. Kept long, the table written much, it keeps alive what was
// written over. Of a Set without ReadConcurrency, it is a copy of the
// entries.
//
// It is not the snapshot of the entries a compaction writes on disk.
type Snapshot[K comparable, V any] struct {
	v view[K, V]
}

// view is the entries of an index at a moment, to read without lock.
type view[K comparable, V any] interface {
	// get returns the value of k, and the seq of the write that set it.
	get(k K) (V, uint64, bool)
	len() int
	// each yields the entries, in the order of their keys for a tree.
	each(yield func(*leaf[K, V]) bool)
}

// Snapshot returns the table as it is now: see Snapshot. Taking it waits
// for the writes under way, and holds the others back the while.
func (t *Table[K, V]) Snapshot() Snapshot[K, V] {
	return Snapshot[K, V]{v: t.idx.snapshot()}
}

// Get returns the value under k.
func (s Snapshot[K, V]) Get(k K) (V, bool) {
	v, _, ok := s.v.get(k)
	return v, ok
}

// GetRev returns the value under k and its revision, as Table.GetRev.
func (s Snapshot[K, V]) GetRev(k K) (V, uint64, bool) { return s.v.get(k) }

// Len returns the number of entries.
func (s Snapshot[K, V]) Len() int { return s.v.len() }

// All returns the entries, in the order of their keys in an OrderedSet.
func (s Snapshot[K, V]) All() iter.Seq2[K, V] { return s.Select(nil) }

// Select returns the entries for which keep, if not nil, reports true, as
// All does.
func (s Snapshot[K, V]) Select(keep func(K, V) bool) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		s.v.each(func(l *leaf[K, V]) bool {
			if keep != nil && !keep(l.Key, l.Val) {
				return true
			}
			return yield(l.Key, l.Val)
		})
	}
}

// Range returns the entries of the keys from lo, up to hi but not hi, in
// order. It panics if the table is not an OrderedSet.
func (s Snapshot[K, V]) Range(lo, hi K) iter.Seq2[K, V] {
	t, ok := s.v.(*treeView[K, V])
	if !ok {
		panic("kvs: Range of a Set")
	}
	return func(yield func(K, V) bool) {
		t.x.ascendRoots(t.roots, &lo, &hi, func(l *leaf[K, V]) bool { return yield(l.Key, l.Val) })
	}
}

// shardOf returns the index of the shard of k.
func shardOf[K comparable](h hasher, k K) int {
	if h.bits == 0 {
		return 0
	}
	return int(h.shard(maphash.Comparable(h.seed, k)))
}

// treeView is the trees of the shards of a treeIndex at a moment.
type treeView[K comparable, V any] struct {
	x     *treeIndex[K, V]
	roots []*bnode[K, V]
	size  int
}

// snapshot locks all the shards, in order, to take their trees at one
// moment: without ReadConcurrency, a new generation, for the writes after
// to copy the nodes of the trees taken rather than change them.
func (x *treeIndex[K, V]) snapshot() view[K, V] {
	for i := range x.shards {
		x.shards[i].mu.Lock()
	}
	v := &treeView[K, V]{x: x, roots: make([]*bnode[K, V], len(x.shards))}
	for i := range x.shards {
		s := &x.shards[i]
		if !x.lockless {
			s.gen++
		}
		v.roots[i], v.size = s.root.Load(), v.size+int(s.size.Load())
	}
	for i := range x.shards {
		x.shards[i].mu.Unlock()
	}
	return v
}

func (v *treeView[K, V]) get(k K) (V, uint64, bool) {
	l, ok := (&btree[K, V]{cmp: v.x.cmp, root: v.roots[shardOf(v.x.hasher, k)]}).get(k)
	if !ok {
		var zero V
		return zero, 0, false
	}
	return l.Val, l.Meta.rev, true
}

func (v *treeView[K, V]) len() int { return v.size }

func (v *treeView[K, V]) each(yield func(*leaf[K, V]) bool) {
	v.x.ascendRoots(v.roots, nil, nil, yield)
}

// hamtView is the tries of the shards of a hamtIndex at a moment.
type hamtView[K comparable, V any] struct {
	hasher
	roots []*hnode[K, V]
	size  int
}

// snapshot locks all the shards, in order, to take their tries at one
// moment.
func (x *hamtIndex[K, V]) snapshot() view[K, V] {
	for i := range x.shards {
		x.shards[i].mu.Lock()
	}
	v := &hamtView[K, V]{hasher: x.hasher, roots: make([]*hnode[K, V], len(x.shards))}
	for i := range x.shards {
		s := &x.shards[i]
		v.roots[i], v.size = s.root.Load(), v.size+int(s.size.Load())
	}
	for i := range x.shards {
		x.shards[i].mu.Unlock()
	}
	return v
}

func (v *hamtView[K, V]) get(k K) (V, uint64, bool) {
	h := maphash.Comparable(v.seed, k)
	l, ok := v.roots[v.shard(h)].Get(h, k)
	if !ok {
		var zero V
		return zero, 0, false
	}
	return l.Val, l.Meta.rev, true
}

func (v *hamtView[K, V]) len() int { return v.size }

func (v *hamtView[K, V]) each(yield func(*leaf[K, V]) bool) {
	for _, root := range v.roots {
		if !root.Each(yield) {
			return
		}
	}
}

// mapView is a copy of the maps of the shards of a mapIndex at a moment.
type mapView[K comparable, V any] struct {
	hasher
	maps []map[K]entry[V]
	size int
}

// snapshot locks all the shards, in order, to copy their maps at one
// moment.
func (x *mapIndex[K, V]) snapshot() view[K, V] {
	for i := range x.shards {
		x.shards[i].mu.RLock()
	}
	v := &mapView[K, V]{hasher: x.hasher, maps: make([]map[K]entry[V], len(x.shards))}
	for i := range x.shards {
		m := maps.Clone(x.shards[i].m)
		v.maps[i], v.size = m, v.size+len(m)
	}
	for i := range x.shards {
		x.shards[i].mu.RUnlock()
	}
	return v
}

func (v *mapView[K, V]) get(k K) (V, uint64, bool) {
	e, ok := v.maps[shardOf(v.hasher, k)][k]
	return e.val, e.rev, ok
}

func (v *mapView[K, V]) len() int { return v.size }

func (v *mapView[K, V]) each(yield func(*leaf[K, V]) bool) {
	for _, m := range v.maps {
		for k, e := range m {
			l := leaf[K, V]{Key: k, Val: e.val, Meta: meta{size: e.size, rev: e.rev}}
			if !yield(&l) {
				return
			}
		}
	}
}
