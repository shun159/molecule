package kvs

import (
	"hash/maphash"
	"iter"
	"sync/atomic"

	"github.com/shun159/molecule/internal/champ"
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
	// byShard returns each shard in turn, as the entries it holds, to
	// range over without lock. With cut, they are those it holds once
	// locked, for a compaction.
	byShard(cut bool) iter.Seq[entries[K, V]]
	// snapshot returns the entries as they are now, in all the shards at
	// one moment, to read without lock: see Snapshot.
	snapshot() view[K, V]
	// load sets or deletes entries on opening, without the log.
	load(k K, v V, size int64, rev uint64)
	unload(k K)
}

// leaf is an entry of an index: a key, its value, and the size and seq of
// the record that wrote it in the log; and its hash, of the hash trie.
type leaf[K comparable, V any] = champ.Entry[K, V, meta]

// meta is what an index keeps of an entry beside its key and value.
type meta struct {
	size int64  // of the record that wrote it in the log
	rev  uint64 // the seq of that record
}

// entries are the entries of a shard, to range over.
type entries[K comparable, V any] = iter.Seq[*leaf[K, V]]

// pointers ranges over ls, by pointer.
func pointers[T any](ls []T) iter.Seq[*T] {
	return func(yield func(*T) bool) {
		for i := range ls {
			if !yield(&ls[i]) {
				return
			}
		}
	}
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
