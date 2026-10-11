package kvs

import (
	"math/bits"
	"slices"
)

// A persistent hash trie, CHAMP: a node is never changed once made, so a
// reader holding a root reads one version of the table, without locks,
// while a writer makes the next by copying the path to what it changes.
//
// A node at shift s places an entry by the 5 bits of its hash from s: in
// data if the entry is alone there, in nodes, a node at s+5, if several
// are. Past the 64 bits of the hash, a node lists the entries of one hash
// in data. A node below the root holds two entries or more, or a node:
// one left with a single entry goes up into its parent.

const (
	hbits = 5
	hmask = 1<<hbits - 1
)

// leaf is an entry: a key, its hash and value, and the size of the record
// that wrote it in the log.
type leaf[K comparable, V any] struct {
	hash uint64
	key  K
	val  V
	size int64
}

type hnode[K comparable, V any] struct {
	datamap, nodemap uint32
	data             []leaf[K, V]
	nodes            []*hnode[K, V]
}

func slot(bitmap, bit uint32) int { return bits.OnesCount32(bitmap & (bit - 1)) }

func (n *hnode[K, V]) get(h uint64, k K) (*leaf[K, V], bool) {
	for shift := uint(0); ; shift += hbits {
		if shift >= 64 {
			for i := range n.data {
				if n.data[i].key == k {
					return &n.data[i], true
				}
			}
			return nil, false
		}
		bit := uint32(1) << ((h >> shift) & hmask)
		if n.datamap&bit != 0 {
			l := &n.data[slot(n.datamap, bit)]
			if l.hash == h && l.key == k {
				return l, true
			}
			return nil, false
		}
		if n.nodemap&bit == 0 {
			return nil, false
		}
		n = n.nodes[slot(n.nodemap, bit)]
	}
}

// set returns n with l in it, and the entry of its key that l replaces, if
// any.
func (n *hnode[K, V]) set(shift uint, l leaf[K, V]) (*hnode[K, V], leaf[K, V], bool) {
	var none leaf[K, V]
	if shift >= 64 {
		for i := range n.data {
			if n.data[i].key == l.key {
				c := &hnode[K, V]{data: replaced(n.data, i, l)}
				return c, n.data[i], true
			}
		}
		return &hnode[K, V]{data: inserted(n.data, len(n.data), l)}, none, false
	}
	bit := uint32(1) << ((l.hash >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := slot(n.datamap, bit)
		cur := n.data[i]
		if cur.hash == l.hash && cur.key == l.key {
			c := &hnode[K, V]{datamap: n.datamap, nodemap: n.nodemap, data: replaced(n.data, i, l), nodes: n.nodes}
			return c, cur, true
		}
		c := &hnode[K, V]{datamap: n.datamap &^ bit, nodemap: n.nodemap | bit}
		c.data = removed(n.data, i)
		c.nodes = inserted(n.nodes, slot(c.nodemap, bit), pair(shift+hbits, cur, l))
		return c, none, false
	case n.nodemap&bit != 0:
		i := slot(n.nodemap, bit)
		sub, old, ok := n.nodes[i].set(shift+hbits, l)
		c := &hnode[K, V]{datamap: n.datamap, nodemap: n.nodemap, data: n.data, nodes: replaced(n.nodes, i, sub)}
		return c, old, ok
	default:
		c := &hnode[K, V]{datamap: n.datamap | bit, nodemap: n.nodemap, nodes: n.nodes}
		c.data = inserted(n.data, slot(c.datamap, bit), l)
		return c, none, false
	}
}

// setInPlace puts l in n, changing n and the nodes under it, which no one
// else may hold: for a trie being built, as on opening.
func (n *hnode[K, V]) setInPlace(shift uint, l leaf[K, V]) (leaf[K, V], bool) {
	var none leaf[K, V]
	for ; shift < 64; shift += hbits {
		bit := uint32(1) << ((l.hash >> shift) & hmask)
		switch {
		case n.datamap&bit != 0:
			i := slot(n.datamap, bit)
			cur := n.data[i]
			if cur.hash == l.hash && cur.key == l.key {
				n.data[i] = l
				return cur, true
			}
			n.datamap &^= bit
			n.data = slices.Delete(n.data, i, i+1)
			n.nodemap |= bit
			n.nodes = slices.Insert(n.nodes, slot(n.nodemap, bit), pair(shift+hbits, cur, l))
			return none, false
		case n.nodemap&bit != 0:
			n = n.nodes[slot(n.nodemap, bit)]
		default:
			n.datamap |= bit
			n.data = slices.Insert(n.data, slot(n.datamap, bit), l)
			return none, false
		}
	}
	for i := range n.data {
		if n.data[i].key == l.key {
			cur := n.data[i]
			n.data[i] = l
			return cur, true
		}
	}
	n.data = append(n.data, l)
	return none, false
}

// pair makes the node at shift of two entries of different keys.
func pair[K comparable, V any](shift uint, a, b leaf[K, V]) *hnode[K, V] {
	if shift >= 64 {
		return &hnode[K, V]{data: []leaf[K, V]{a, b}}
	}
	ia, ib := (a.hash>>shift)&hmask, (b.hash>>shift)&hmask
	if ia == ib {
		return &hnode[K, V]{nodemap: 1 << ia, nodes: []*hnode[K, V]{pair(shift+hbits, a, b)}}
	}
	if ia > ib {
		a, b = b, a
	}
	return &hnode[K, V]{datamap: 1<<ia | 1<<ib, data: []leaf[K, V]{a, b}}
}

// del returns n without the entry of k, and that entry, if any.
func (n *hnode[K, V]) del(shift uint, h uint64, k K) (*hnode[K, V], leaf[K, V], bool) {
	var none leaf[K, V]
	if shift >= 64 {
		for i := range n.data {
			if n.data[i].key == k {
				return &hnode[K, V]{data: removed(n.data, i)}, n.data[i], true
			}
		}
		return n, none, false
	}
	bit := uint32(1) << ((h >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := slot(n.datamap, bit)
		cur := n.data[i]
		if cur.hash != h || cur.key != k {
			return n, none, false
		}
		c := &hnode[K, V]{datamap: n.datamap &^ bit, nodemap: n.nodemap, data: removed(n.data, i), nodes: n.nodes}
		return c, cur, true
	case n.nodemap&bit != 0:
		i := slot(n.nodemap, bit)
		sub, old, ok := n.nodes[i].del(shift+hbits, h, k)
		if !ok {
			return n, none, false
		}
		if len(sub.data) == 1 && len(sub.nodes) == 0 {
			// Alone, the entry left goes up.
			c := &hnode[K, V]{datamap: n.datamap | bit, nodemap: n.nodemap &^ bit}
			c.data = inserted(n.data, slot(c.datamap, bit), sub.data[0])
			c.nodes = removed(n.nodes, i)
			return c, old, true
		}
		c := &hnode[K, V]{datamap: n.datamap, nodemap: n.nodemap, data: n.data, nodes: replaced(n.nodes, i, sub)}
		return c, old, true
	}
	return n, none, false
}

// each yields the entries under n, until yield returns false.
func (n *hnode[K, V]) each(yield func(*leaf[K, V]) bool) bool {
	for i := range n.data {
		if !yield(&n.data[i]) {
			return false
		}
	}
	for _, c := range n.nodes {
		if !c.each(yield) {
			return false
		}
	}
	return true
}

// The slices of a node are never changed: these make new ones.

func inserted[T any](s []T, i int, v T) []T {
	c := make([]T, len(s)+1)
	copy(c, s[:i])
	c[i] = v
	copy(c[i+1:], s[i:])
	return c
}

func removed[T any](s []T, i int) []T {
	if len(s) == 1 {
		return nil
	}
	c := make([]T, len(s)-1)
	copy(c, s[:i])
	copy(c[i:], s[i+1:])
	return c
}

func replaced[T any](s []T, i int, v T) []T {
	c := make([]T, len(s))
	copy(c, s)
	c[i] = v
	return c
}
