// Package pmap is a map never changed once made, as the maps of Erlang:
// Set and Delete return a new map, which shares all but the path to what
// changed with the old, the old staying as it was. A pure behaviour keeps
// one in its state, and changes a large one at the cost of a few nodes,
// not of a copy of it all.
//
//	m := pmap.Map[string, int]{}
//	m2 := m.Set("a", 1) // m is still empty
//	v, ok := m2.Get("a")
//
// The zero Map is empty and ready. The keys are hashed with a seed of each
// map, drawn as it gets its first entry: two maps of the same entries list
// them in different orders, and a replicated state machine must not
// depend on the order of All.
package pmap

import (
	"hash/maphash"
	"iter"

	"github.com/shun159/molecule/internal/champ"
)

// node and entry are of the hash trie a map holds the root of, as kvs's.
type (
	node[K comparable, V any]  = champ.Node[K, V, struct{}]
	entry[K comparable, V any] = champ.Entry[K, V, struct{}]
)

// Map is a map of K to V, never changed: see the package documentation.
type Map[K comparable, V any] struct {
	root *node[K, V]
	n    int
	seed maphash.Seed
}

// Len returns how many entries m has.
func (m Map[K, V]) Len() int { return m.n }

// Get returns the value of k in m, if any.
func (m Map[K, V]) Get(k K) (V, bool) {
	if m.root != nil {
		if e, ok := m.root.Get(maphash.Comparable(m.seed, k), k); ok {
			return e.Val, true
		}
	}
	var zero V
	return zero, false
}

// Set returns m with k set to v.
func (m Map[K, V]) Set(k K, v V) Map[K, V] {
	if m.root == nil {
		m.root, m.seed = &node[K, V]{}, maphash.MakeSeed()
	}
	root, _, replaced := m.root.Set(entry[K, V]{Hash: maphash.Comparable(m.seed, k), Key: k, Val: v})
	m.root = root
	if !replaced {
		m.n++
	}
	return m
}

// Delete returns m without k.
func (m Map[K, V]) Delete(k K) Map[K, V] {
	if m.root == nil {
		return m
	}
	root, _, deleted := m.root.Delete(maphash.Comparable(m.seed, k), k)
	if deleted {
		m.root, m.n = root, m.n-1
	}
	return m
}

// All yields the entries of m, in no order that means anything.
func (m Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if m.root != nil {
			m.root.Each(func(e *entry[K, V]) bool { return yield(e.Key, e.Val) })
		}
	}
}

// Collect returns a map of the entries of seq, the last of a key winning:
// built in place, faster than by Set.
func Collect[K comparable, V any](seq iter.Seq2[K, V]) Map[K, V] {
	m := Map[K, V]{root: &node[K, V]{}, seed: maphash.MakeSeed()}
	for k, v := range seq {
		if _, replaced := m.root.SetInPlace(entry[K, V]{Hash: maphash.Comparable(m.seed, k), Key: k, Val: v}); !replaced {
			m.n++
		}
	}
	return m
}
