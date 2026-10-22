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
//
//abc:hot kvs/BenchmarkGet,pmap/BenchmarkGet
func (n *Node[K, V, M]) Get(h uint64, k K) (*Entry[K, V, M], bool) {
	for shift := uint(0); shift < 64; shift += hbits {
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
	return n.getCollided(k)
}

// getCollided returns the entry of k in n, past the bits of the hash, if
// any.
func (n *Node[K, V, M]) getCollided(k K) (*Entry[K, V, M], bool) {
	for i := range n.data {
		if n.data[i].Key == k {
			return &n.data[i], true
		}
	}
	return nil, false
}

//abc:hot kvs/BenchmarkPut,pmap/BenchmarkSet
func (n *Node[K, V, M]) set(shift uint, e Entry[K, V, M]) (*Node[K, V, M], Entry[K, V, M], bool) {
	var none Entry[K, V, M]
	if shift >= 64 {
		return n.setCollided(e)
	}
	bit := uint32(1) << ((e.Hash >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := n.dataSlot(bit)
		cur := n.data[i]
		if cur.Hash == e.Hash && cur.Key == e.Key {
			return n.withReplaced(i, e), cur, true
		}
		return n.pushedDown(shift, bit, i, e), none, false
	case n.nodemap&bit != 0:
		i := n.nodeSlot(bit)
		sub, old, ok := n.nodes[i].set(shift+hbits, e)
		return n.withNode(i, sub), old, ok
	default:
		return n.withEntry(bit, e), none, false
	}
}

// setCollided returns n, past the bits of the hash, with e in it, and the
// entry of its key that e replaces, if any.
func (n *Node[K, V, M]) setCollided(e Entry[K, V, M]) (*Node[K, V, M], Entry[K, V, M], bool) {
	for i := range n.data {
		if n.data[i].Key == e.Key {
			return n.withReplaced(i, e), n.data[i], true
		}
	}
	c := newNode[K, V, M](0, 0, len(n.data)+1, 0)
	insertedInto(c.data, n.data, len(n.data), e)
	var none Entry[K, V, M]
	return c, none, false
}

//abc:hot kvs/BenchmarkOpen
func (n *Node[K, V, M]) setInPlace(shift uint, e Entry[K, V, M]) (Entry[K, V, M], bool) {
	var none Entry[K, V, M]
	for ; shift < 64; shift += hbits {
		bit := uint32(1) << ((e.Hash >> shift) & hmask)
		switch {
		case n.datamap&bit != 0:
			return n.setOverInPlace(shift, bit, e)
		case n.nodemap&bit != 0:
			n = n.nodes[n.nodeSlot(bit)]
		default:
			n.datamap |= bit
			n.data = slices.Insert(n.data, n.dataSlot(bit), e)
			return none, false
		}
	}
	return n.setCollidedInPlace(e)
}

// setOverInPlace puts e in place of the entry at bit of n, if of its key,
// or in a node pushed down at shift with it, if not.
func (n *Node[K, V, M]) setOverInPlace(shift uint, bit uint32, e Entry[K, V, M]) (Entry[K, V, M], bool) {
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
	var none Entry[K, V, M]
	return none, false
}

// setCollidedInPlace puts e in n, past the bits of the hash.
func (n *Node[K, V, M]) setCollidedInPlace(e Entry[K, V, M]) (Entry[K, V, M], bool) {
	for i := range n.data {
		if n.data[i].Key == e.Key {
			cur := n.data[i]
			n.data[i] = e
			return cur, true
		}
	}
	n.data = append(n.data, e)
	var none Entry[K, V, M]
	return none, false
}

// pair makes the node at shift of two entries of different keys.
func pair[K comparable, V, M any](shift uint, a, b Entry[K, V, M]) *Node[K, V, M] {
	if shift >= 64 {
		c := newNode[K, V, M](0, 0, 2, 0)
		c.data[0], c.data[1] = a, b
		return c
	}
	ia, ib := (a.Hash>>shift)&hmask, (b.Hash>>shift)&hmask
	if ia == ib {
		c := newNode[K, V, M](0, 1<<ia, 0, 1)
		c.nodes[0] = pair(shift+hbits, a, b)
		return c
	}
	if ia > ib {
		a, b = b, a
	}
	c := newNode[K, V, M](1<<ia|1<<ib, 0, 2, 0)
	c.data[0], c.data[1] = a, b
	return c
}

func (n *Node[K, V, M]) del(shift uint, h uint64, k K) (*Node[K, V, M], Entry[K, V, M], bool) {
	var none Entry[K, V, M]
	if shift >= 64 {
		return n.delCollided(k)
	}
	bit := uint32(1) << ((h >> shift) & hmask)
	switch {
	case n.datamap&bit != 0:
		i := n.dataSlot(bit)
		cur := n.data[i]
		if cur.Hash != h || cur.Key != k {
			return n, none, false
		}
		return n.withoutEntry(bit, i), cur, true
	case n.nodemap&bit != 0:
		i := n.nodeSlot(bit)
		sub, old, ok := n.nodes[i].del(shift+hbits, h, k)
		switch {
		case !ok:
			return n, none, false
		case len(sub.data) == 1 && len(sub.nodes) == 0:
			// Alone, the entry left goes up.
			return n.pulledUp(bit, i, sub.data[0]), old, true
		}
		return n.withNode(i, sub), old, true
	}
	return n, none, false
}

// delCollided returns n, past the bits of the hash, without the entry of
// k, and that entry, if any.
func (n *Node[K, V, M]) delCollided(k K) (*Node[K, V, M], Entry[K, V, M], bool) {
	for i := range n.data {
		if n.data[i].Key == k {
			c := newNode[K, V, M](0, 0, len(n.data)-1, 0)
			removedInto(c.data, n.data, i)
			return c, n.data[i], true
		}
	}
	var none Entry[K, V, M]
	return n, none, false
}

// The nodes a write makes of n: n but for what the write changes. Its
// data are shared only by withNode, of a node with nodes: the data of a
// node with none are in its room, and shared, would keep alive that node
// and the entries it held. Its nodes are always new, made with it, for
// the same reason.

// withReplaced is n with e in place of its entry i.
func (n *Node[K, V, M]) withReplaced(i int, e Entry[K, V, M]) *Node[K, V, M] {
	c := newNode[K, V, M](n.datamap, n.nodemap, len(n.data), len(n.nodes))
	replacedInto(c.data, n.data, i, e)
	copy(c.nodes, n.nodes)
	return c
}

// withNode is n with sub as its node i.
func (n *Node[K, V, M]) withNode(i int, sub *Node[K, V, M]) *Node[K, V, M] {
	c := newNode[K, V, M](n.datamap, n.nodemap, 0, len(n.nodes))
	c.data = n.data
	replacedInto(c.nodes, n.nodes, i, sub)
	return c
}

// withEntry is n with e at bit, where n has nothing.
func (n *Node[K, V, M]) withEntry(bit uint32, e Entry[K, V, M]) *Node[K, V, M] {
	c := newNode[K, V, M](n.datamap|bit, n.nodemap, len(n.data)+1, len(n.nodes))
	insertedInto(c.data, n.data, c.dataSlot(bit), e)
	copy(c.nodes, n.nodes)
	return c
}

// withoutEntry is n without its entry i, at bit.
func (n *Node[K, V, M]) withoutEntry(bit uint32, i int) *Node[K, V, M] {
	c := newNode[K, V, M](n.datamap&^bit, n.nodemap, len(n.data)-1, len(n.nodes))
	removedInto(c.data, n.data, i)
	copy(c.nodes, n.nodes)
	return c
}

// pushedDown is n with its entry i, at bit, and e, of another key, in a
// node of the next shift in its place.
func (n *Node[K, V, M]) pushedDown(shift uint, bit uint32, i int, e Entry[K, V, M]) *Node[K, V, M] {
	c := newNode[K, V, M](n.datamap&^bit, n.nodemap|bit, len(n.data)-1, len(n.nodes)+1)
	removedInto(c.data, n.data, i)
	insertedInto(c.nodes, n.nodes, c.nodeSlot(bit), pair(shift+hbits, n.data[i], e))
	return c
}

// pulledUp is n with e, the entry left alone in its node i at bit, in
// place of that node.
func (n *Node[K, V, M]) pulledUp(bit uint32, i int, e Entry[K, V, M]) *Node[K, V, M] {
	c := newNode[K, V, M](n.datamap|bit, n.nodemap&^bit, len(n.data)+1, len(n.nodes)-1)
	insertedInto(c.data, n.data, c.dataSlot(bit), e)
	removedInto(c.nodes, n.nodes, i)
	return c
}

// newNode makes a node of the bitmaps given, and of entries entries and
// nodes nodes, to fill. As a write copies a node at each level of its
// path, the node comes in one allocation with what is most of it: its
// nodes, if it has any, or its entries, if not.
func newNode[K comparable, V, M any](datamap, nodemap uint32, entries, nodes int) *Node[K, V, M] {
	var c *Node[K, V, M]
	if nodes == 0 {
		c = newLeaf[K, V, M](entries)
	} else {
		c = newBranch[K, V, M](nodes)
		if entries > 0 {
			c.data = make([]Entry[K, V, M], entries)
		}
	}
	c.datamap, c.nodemap = datamap, nodemap
	return c
}

// newBranch makes a node with room for nodes nodes, the least of a few
// sizes that holds them.
func newBranch[K comparable, V, M any](nodes int) *Node[K, V, M] {
	switch {
	case nodes == 1:
		r := new(nodeRoom1[K, V, M])
		r.nodes = r.room[:]
		return &r.Node
	case nodes <= 2:
		r := new(nodeRoom2[K, V, M])
		r.nodes = r.room[:nodes:nodes]
		return &r.Node
	case nodes <= 4:
		r := new(nodeRoom4[K, V, M])
		r.nodes = r.room[:nodes:nodes]
		return &r.Node
	case nodes <= 8:
		r := new(nodeRoom8[K, V, M])
		r.nodes = r.room[:nodes:nodes]
		return &r.Node
	case nodes <= 16:
		r := new(nodeRoom16[K, V, M])
		r.nodes = r.room[:nodes:nodes]
		return &r.Node
	default:
		r := new(nodeRoom32[K, V, M])
		r.nodes = r.room[:nodes:nodes]
		return &r.Node
	}
}

// newLeaf makes a node of no nodes with room for entries entries, the
// least of a few sizes that holds them. The sizes are closer than those
// of nodes, as an entry is larger than a pointer, and most nodes of no
// nodes hold 2 to 6 entries. One of a single entry, a root, takes the
// room of 2; one of more than 16, rare, has its entries apart.
func newLeaf[K comparable, V, M any](entries int) *Node[K, V, M] {
	switch {
	case entries == 0:
		return &Node[K, V, M]{}
	case entries <= 2:
		r := new(leafRoom2[K, V, M])
		r.data = r.room[:entries:entries]
		return &r.Node
	case entries == 3:
		r := new(leafRoom3[K, V, M])
		r.data = r.room[:]
		return &r.Node
	case entries == 4:
		r := new(leafRoom4[K, V, M])
		r.data = r.room[:]
		return &r.Node
	case entries <= 6:
		r := new(leafRoom6[K, V, M])
		r.data = r.room[:entries:entries]
		return &r.Node
	case entries <= 8:
		r := new(leafRoom8[K, V, M])
		r.data = r.room[:entries:entries]
		return &r.Node
	case entries <= 12:
		r := new(leafRoom12[K, V, M])
		r.data = r.room[:entries:entries]
		return &r.Node
	case entries <= 16:
		r := new(leafRoom16[K, V, M])
		r.data = r.room[:entries:entries]
		return &r.Node
	default:
		c := &Node[K, V, M]{}
		c.data = make([]Entry[K, V, M], entries)
		return c
	}
}

// A node and the room for its nodes, or for its entries, allocated
// together.
type (
	nodeRoom1[K comparable, V, M any] struct {
		Node[K, V, M]
		room [1]*Node[K, V, M]
	}
	nodeRoom2[K comparable, V, M any] struct {
		Node[K, V, M]
		room [2]*Node[K, V, M]
	}
	nodeRoom4[K comparable, V, M any] struct {
		Node[K, V, M]
		room [4]*Node[K, V, M]
	}
	nodeRoom8[K comparable, V, M any] struct {
		Node[K, V, M]
		room [8]*Node[K, V, M]
	}
	nodeRoom16[K comparable, V, M any] struct {
		Node[K, V, M]
		room [16]*Node[K, V, M]
	}
	nodeRoom32[K comparable, V, M any] struct {
		Node[K, V, M]
		room [32]*Node[K, V, M]
	}

	leafRoom2[K comparable, V, M any] struct {
		Node[K, V, M]
		room [2]Entry[K, V, M]
	}
	leafRoom3[K comparable, V, M any] struct {
		Node[K, V, M]
		room [3]Entry[K, V, M]
	}
	leafRoom4[K comparable, V, M any] struct {
		Node[K, V, M]
		room [4]Entry[K, V, M]
	}
	leafRoom6[K comparable, V, M any] struct {
		Node[K, V, M]
		room [6]Entry[K, V, M]
	}
	leafRoom8[K comparable, V, M any] struct {
		Node[K, V, M]
		room [8]Entry[K, V, M]
	}
	leafRoom12[K comparable, V, M any] struct {
		Node[K, V, M]
		room [12]Entry[K, V, M]
	}
	leafRoom16[K comparable, V, M any] struct {
		Node[K, V, M]
		room [16]Entry[K, V, M]
	}
)

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

// The slices of a node are never changed: these fill c, made for them,
// from s.

func insertedInto[T any](c, s []T, i int, v T) []T {
	copy(c, s[:i])
	c[i] = v
	copy(c[i+1:], s[i:])
	return c
}

func removedInto[T any](c, s []T, i int) []T {
	copy(c, s[:i])
	copy(c[i:], s[i+1:])
	return c
}

func replacedInto[T any](c, s []T, i int, v T) []T {
	copy(c, s)
	c[i] = v
	return c
}
