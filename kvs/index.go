package kvs

import (
	"hash/maphash"
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

// newIndex makes the index of a table: a B-tree for an OrderedSet, cmp
// given; a trie with ReadConcurrency; a map otherwise. With
// WriteConcurrency, in shards.
func newIndex[K comparable, V any](read, write bool, shards int, cmp func(a, b K) int, live *atomic.Int64) index[K, V] {
	n := 1
	if write {
		n = shards
	}
	h := hasher{seed: maphash.MakeSeed()}
	for n > 1<<h.bits {
		h.bits++
	}
	switch {
	case cmp != nil:
		return newTreeIndex[K, V](h, cmp, read, live)
	case read:
		return newHamtIndex[K, V](h, live)
	default:
		return newMapIndex[K, V](h, live)
	}
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
