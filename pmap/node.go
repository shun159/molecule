package pmap

import (
	"math/bits"
	"slices"
)

// A hash trie, CHAMP, as kvs's: a node is never changed once made, so a
// map holding a root holds one version, while Set and Delete make the next
// by copying the path to what they change.
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

type entry[K comparable, V any] struct {
	hash uint64
	key  K
	val  V
}

type node[K comparable, V any] struct {
	datamap, nodemap uint32
	data             []entry[K, V]
	nodes            []*node[K, V]
}

func slot(bitmap, bit uint32) int { return bits.OnesCount32(bitmap & (bit - 1)) }

func (n *node[K, V]) get(h uint64, k K) (*entry[K, V], bool) {
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
			e := &n.data[slot(n.datamap, bit)]
			if e.hash == h && e.key == k {
				return e, true
			}
			return nil, false
		}
		if n.nodemap&bit == 0 {
			return nil, false
		}
		n = n.nodes[slot(n.nodemap, bit)]
	}
}

// set returns n with e in it, and whether e replaced an entry of its key.
func (n *node[K, V]) set(shift uint, e entry[K, V]) (*node[K, V], bool) {
	if shift >= 64 {
		for i := range n.data {
			if n.data[i].key == e.key {
				return &node[K, V]{data: replaced(n.data, i, e)}, true
			}
		}
		return &node[K, V]{data: inserted(n.data, len(n.data), e)}, false
	}
	bit := uint32(1) << ((e.hash >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := slot(n.datamap, bit)
		cur := n.data[i]
		if cur.hash == e.hash && cur.key == e.key {
			return &node[K, V]{datamap: n.datamap, nodemap: n.nodemap, data: replaced(n.data, i, e), nodes: n.nodes}, true
		}
		c := &node[K, V]{datamap: n.datamap &^ bit, nodemap: n.nodemap | bit}
		c.data = removed(n.data, i)
		c.nodes = inserted(n.nodes, slot(c.nodemap, bit), pair(shift+hbits, cur, e))
		return c, false
	case n.nodemap&bit != 0:
		i := slot(n.nodemap, bit)
		sub, ok := n.nodes[i].set(shift+hbits, e)
		return &node[K, V]{datamap: n.datamap, nodemap: n.nodemap, data: n.data, nodes: replaced(n.nodes, i, sub)}, ok
	default:
		c := &node[K, V]{datamap: n.datamap | bit, nodemap: n.nodemap, nodes: n.nodes}
		c.data = inserted(n.data, slot(c.datamap, bit), e)
		return c, false
	}
}

// setInPlace puts e in n, changing n and the nodes under it, which no map
// holds yet, and reports whether e replaced an entry of its key.
func (n *node[K, V]) setInPlace(shift uint, e entry[K, V]) bool {
	for ; shift < 64; shift += hbits {
		bit := uint32(1) << ((e.hash >> shift) & hmask)
		switch {
		case n.datamap&bit != 0:
			i := slot(n.datamap, bit)
			cur := n.data[i]
			if cur.hash == e.hash && cur.key == e.key {
				n.data[i] = e
				return true
			}
			n.datamap &^= bit
			n.data = slices.Delete(n.data, i, i+1)
			n.nodemap |= bit
			n.nodes = slices.Insert(n.nodes, slot(n.nodemap, bit), pair(shift+hbits, cur, e))
			return false
		case n.nodemap&bit != 0:
			n = n.nodes[slot(n.nodemap, bit)]
		default:
			n.datamap |= bit
			n.data = slices.Insert(n.data, slot(n.datamap, bit), e)
			return false
		}
	}
	for i := range n.data {
		if n.data[i].key == e.key {
			n.data[i] = e
			return true
		}
	}
	n.data = append(n.data, e)
	return false
}

// pair makes the node at shift of two entries of different keys.
func pair[K comparable, V any](shift uint, a, b entry[K, V]) *node[K, V] {
	if shift >= 64 {
		return &node[K, V]{data: []entry[K, V]{a, b}}
	}
	ia, ib := (a.hash>>shift)&hmask, (b.hash>>shift)&hmask
	if ia == ib {
		return &node[K, V]{nodemap: 1 << ia, nodes: []*node[K, V]{pair(shift+hbits, a, b)}}
	}
	if ia > ib {
		a, b = b, a
	}
	return &node[K, V]{datamap: 1<<ia | 1<<ib, data: []entry[K, V]{a, b}}
}

// del returns n without the entry of k, and whether it had one.
func (n *node[K, V]) del(shift uint, h uint64, k K) (*node[K, V], bool) {
	if shift >= 64 {
		for i := range n.data {
			if n.data[i].key == k {
				return &node[K, V]{data: removed(n.data, i)}, true
			}
		}
		return n, false
	}
	bit := uint32(1) << ((h >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := slot(n.datamap, bit)
		if cur := n.data[i]; cur.hash != h || cur.key != k {
			return n, false
		}
		return &node[K, V]{datamap: n.datamap &^ bit, nodemap: n.nodemap, data: removed(n.data, i), nodes: n.nodes}, true
	case n.nodemap&bit != 0:
		i := slot(n.nodemap, bit)
		sub, ok := n.nodes[i].del(shift+hbits, h, k)
		if !ok {
			return n, false
		}
		if len(sub.data) == 1 && len(sub.nodes) == 0 {
			// Alone, the entry left goes up.
			c := &node[K, V]{datamap: n.datamap | bit, nodemap: n.nodemap &^ bit}
			c.data = inserted(n.data, slot(c.datamap, bit), sub.data[0])
			c.nodes = removed(n.nodes, i)
			return c, true
		}
		return &node[K, V]{datamap: n.datamap, nodemap: n.nodemap, data: n.data, nodes: replaced(n.nodes, i, sub)}, true
	}
	return n, false
}

// each yields the entries under n, until yield returns false.
func (n *node[K, V]) each(yield func(*entry[K, V]) bool) bool {
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
