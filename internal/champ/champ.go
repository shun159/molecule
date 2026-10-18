// Package champ is a persistent hash trie, CHAMP, of kvs and pmap: a node
// is never changed once made, so a reader holding a root reads one
// version, without locks, while a writer makes the next by copying the
// path to what it changes.
//
// A node at shift s places an entry by the 5 bits of its hash from s: in
// data if the entry is alone there, in nodes, a node at s+5, if several
// are. Past the 64 bits of the hash, a node lists the entries of one hash
// in data. A node below the root holds two entries or more, or a node:
// one left with a single entry goes up into its parent.
package champ

import (
	"math/bits"
	"slices"
)

const (
	hbits = 5
	hmask = 1<<hbits - 1
)

// An Entry is a key, its hash and value, and what its user keeps of it
// beside: kvs the size and seq of the record that wrote it, pmap nothing.
// Meta comes first: a field of no size last would be padded, and an entry
// of pmap a word larger.
type Entry[K comparable, V, M any] struct {
	Meta M
	Hash uint64
	Key  K
	Val  V
}

// A Node is a node of a trie; the zero Node is an empty root.
type Node[K comparable, V, M any] struct {
	datamap, nodemap uint32
	data             []Entry[K, V, M]
	nodes            []*Node[K, V, M]
}

// Set returns n with e in it, and the entry of its key that e replaces, if
// any.
func (n *Node[K, V, M]) Set(e Entry[K, V, M]) (*Node[K, V, M], Entry[K, V, M], bool) {
	return n.set(0, e)
}

// SetInPlace puts e in n, changing n and the nodes under it, which no one
// else may hold: for a trie being built. It returns the entry of its key
// that e replaces, if any.
func (n *Node[K, V, M]) SetInPlace(e Entry[K, V, M]) (Entry[K, V, M], bool) {
	return n.setInPlace(0, e)
}

// Delete returns n without the entry of k, of hash h, and that entry, if
// any.
func (n *Node[K, V, M]) Delete(h uint64, k K) (*Node[K, V, M], Entry[K, V, M], bool) {
	return n.del(0, h, k)
}

// dataSlot and nodeSlot are the places of bit, of a hash, in data and in
// nodes: the bits set below it in its bitmap.
func (n *Node[K, V, M]) dataSlot(bit uint32) int { return bits.OnesCount32(n.datamap & (bit - 1)) }
func (n *Node[K, V, M]) nodeSlot(bit uint32) int { return bits.OnesCount32(n.nodemap & (bit - 1)) }

// Get returns the entry of k, of hash h, if any.
func (n *Node[K, V, M]) Get(h uint64, k K) (*Entry[K, V, M], bool) {
	for shift := uint(0); ; shift += hbits {
		if shift >= 64 {
			for i := range n.data {
				if n.data[i].Key == k {
					return &n.data[i], true
				}
			}
			return nil, false
		}
		bit := uint32(1) << ((h >> shift) & hmask)
		if n.datamap&bit != 0 {
			e := &n.data[n.dataSlot(bit)]
			if e.Hash == h && e.Key == k {
				return e, true
			}
			return nil, false
		}
		if n.nodemap&bit == 0 {
			return nil, false
		}
		n = n.nodes[n.nodeSlot(bit)]
	}
}

func (n *Node[K, V, M]) set(shift uint, e Entry[K, V, M]) (*Node[K, V, M], Entry[K, V, M], bool) {
	var none Entry[K, V, M]
	if shift >= 64 {
		for i := range n.data {
			if n.data[i].Key == e.Key {
				c := &Node[K, V, M]{data: replaced(n.data, i, e)}
				return c, n.data[i], true
			}
		}
		return &Node[K, V, M]{data: inserted(n.data, len(n.data), e)}, none, false
	}
	bit := uint32(1) << ((e.Hash >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := n.dataSlot(bit)
		cur := n.data[i]
		if cur.Hash == e.Hash && cur.Key == e.Key {
			c := &Node[K, V, M]{datamap: n.datamap, nodemap: n.nodemap, data: replaced(n.data, i, e), nodes: n.nodes}
			return c, cur, true
		}
		c := &Node[K, V, M]{datamap: n.datamap &^ bit, nodemap: n.nodemap | bit}
		c.data = removed(n.data, i)
		c.nodes = inserted(n.nodes, c.nodeSlot(bit), pair(shift+hbits, cur, e))
		return c, none, false
	case n.nodemap&bit != 0:
		i := n.nodeSlot(bit)
		sub, old, ok := n.nodes[i].set(shift+hbits, e)
		c := &Node[K, V, M]{datamap: n.datamap, nodemap: n.nodemap, data: n.data, nodes: replaced(n.nodes, i, sub)}
		return c, old, ok
	default:
		c := &Node[K, V, M]{datamap: n.datamap | bit, nodemap: n.nodemap, nodes: n.nodes}
		c.data = inserted(n.data, c.dataSlot(bit), e)
		return c, none, false
	}
}

func (n *Node[K, V, M]) setInPlace(shift uint, e Entry[K, V, M]) (Entry[K, V, M], bool) {
	var none Entry[K, V, M]
	for ; shift < 64; shift += hbits {
		bit := uint32(1) << ((e.Hash >> shift) & hmask)
		switch {
		case n.datamap&bit != 0:
			i := n.dataSlot(bit)
			cur := n.data[i]
			if cur.Hash == e.Hash && cur.Key == e.Key {
				n.data[i] = e
				return cur, true
			}
			n.datamap &^= bit
			n.data = slices.Delete(n.data, i, i+1)
			n.nodemap |= bit
			n.nodes = slices.Insert(n.nodes, n.nodeSlot(bit), pair(shift+hbits, cur, e))
			return none, false
		case n.nodemap&bit != 0:
			n = n.nodes[n.nodeSlot(bit)]
		default:
			n.datamap |= bit
			n.data = slices.Insert(n.data, n.dataSlot(bit), e)
			return none, false
		}
	}
	for i := range n.data {
		if n.data[i].Key == e.Key {
			cur := n.data[i]
			n.data[i] = e
			return cur, true
		}
	}
	n.data = append(n.data, e)
	return none, false
}

// pair makes the node at shift of two entries of different keys.
func pair[K comparable, V, M any](shift uint, a, b Entry[K, V, M]) *Node[K, V, M] {
	if shift >= 64 {
		return &Node[K, V, M]{data: []Entry[K, V, M]{a, b}}
	}
	ia, ib := (a.Hash>>shift)&hmask, (b.Hash>>shift)&hmask
	if ia == ib {
		return &Node[K, V, M]{nodemap: 1 << ia, nodes: []*Node[K, V, M]{pair(shift+hbits, a, b)}}
	}
	if ia > ib {
		a, b = b, a
	}
	return &Node[K, V, M]{datamap: 1<<ia | 1<<ib, data: []Entry[K, V, M]{a, b}}
}

func (n *Node[K, V, M]) del(shift uint, h uint64, k K) (*Node[K, V, M], Entry[K, V, M], bool) {
	var none Entry[K, V, M]
	if shift >= 64 {
		for i := range n.data {
			if n.data[i].Key == k {
				return &Node[K, V, M]{data: removed(n.data, i)}, n.data[i], true
			}
		}
		return n, none, false
	}
	bit := uint32(1) << ((h >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := n.dataSlot(bit)
		cur := n.data[i]
		if cur.Hash != h || cur.Key != k {
			return n, none, false
		}
		c := &Node[K, V, M]{datamap: n.datamap &^ bit, nodemap: n.nodemap, data: removed(n.data, i), nodes: n.nodes}
		return c, cur, true
	case n.nodemap&bit != 0:
		i := n.nodeSlot(bit)
		sub, old, ok := n.nodes[i].del(shift+hbits, h, k)
		if !ok {
			return n, none, false
		}
		if len(sub.data) == 1 && len(sub.nodes) == 0 {
			// Alone, the entry left goes up.
			c := &Node[K, V, M]{datamap: n.datamap | bit, nodemap: n.nodemap &^ bit}
			c.data = inserted(n.data, c.dataSlot(bit), sub.data[0])
			c.nodes = removed(n.nodes, i)
			return c, old, true
		}
		c := &Node[K, V, M]{datamap: n.datamap, nodemap: n.nodemap, data: n.data, nodes: replaced(n.nodes, i, sub)}
		return c, old, true
	}
	return n, none, false
}

// Each yields the entries under n, until yield returns false.
func (n *Node[K, V, M]) Each(yield func(*Entry[K, V, M]) bool) bool {
	for i := range n.data {
		if !yield(&n.data[i]) {
			return false
		}
	}
	for _, c := range n.nodes {
		if !c.Each(yield) {
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
