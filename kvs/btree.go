package kvs

import "slices"

// A B-tree of entries in the order of their keys, copied on write: a node
// belongs to the generation of the tree that made it, and is changed in
// place by that generation only, copied by a later one. Taking a new
// generation thus freezes the tree as it is: a reader holding its root
// reads that version, without locks, while the writer makes the next,
// copying the path to what it changes once.
//
// A node holds maxItems entries at most, and minItems at least below the
// root; a node not a leaf has a child more than entries, the entries of
// child i lying between items i-1 and i.

const (
	maxItems = 15
	minItems = maxItems / 2
)

type bnode[K comparable, V any] struct {
	gen   uint64
	items []leaf[K, V]
	kids  []*bnode[K, V]
}

// btree is a root and the order of its keys, cmp, which must report 0 for
// equal keys only.
type btree[K comparable, V any] struct {
	cmp  func(a, b K) int
	root *bnode[K, V]
	size int
}

// find returns the index of the first entry of n not before k, and
// whether it is k.
func (n *bnode[K, V]) find(cmp func(a, b K) int, k K) (int, bool) {
	lo, hi := 0, len(n.items)
	for lo < hi {
		m := int(uint(lo+hi) >> 1)
		if cmp(n.items[m].Key, k) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(n.items) && cmp(n.items[lo].Key, k) == 0
}

func (t *btree[K, V]) get(k K) (*leaf[K, V], bool) {
	for n := t.root; n != nil; {
		i, ok := n.find(t.cmp, k)
		if ok {
			return &n.items[i], true
		}
		if len(n.kids) == 0 {
			break
		}
		n = n.kids[i]
	}
	return nil, false
}

// mut returns n, to change in generation gen with more entries put in
// it: n itself, if of gen and with room for them; or a copy of it, with
// room. Out of room in its own generation, n is growing in place, as
// without ReadConcurrency or on opening: its copy has the most room, not
// to be copied again as it grows.
func (n *bnode[K, V]) mut(gen uint64, more int) *bnode[K, V] {
	if n.gen == gen {
		if len(n.items)+more <= cap(n.items) {
			return n
		}
		more = maxItems - len(n.items)
	}
	c := newBnode[K, V](gen, len(n.items), len(n.kids), more)
	copy(c.items, n.items)
	copy(c.kids, n.kids)
	return c
}

func (n *bnode[K, V]) kid(i int, gen uint64) *bnode[K, V] {
	c := n.kids[i].mut(gen, 0)
	n.kids[i] = c
	return c
}

// split cuts n, of generation gen, at its entry i, and returns that entry
// and the node of those after it.
func (n *bnode[K, V]) split(i int, gen uint64) (leaf[K, V], *bnode[K, V]) {
	mid := n.items[i]
	right := newBnode[K, V](gen, len(n.items)-i-1, max(len(n.kids)-i-1, 0), 0)
	copy(right.items, n.items[i+1:])
	clear(n.items[i:])
	n.items = n.items[:i]
	if len(n.kids) > 0 {
		copy(right.kids, n.kids[i+1:])
		clear(n.kids[i+1:])
		n.kids = n.kids[:i+1]
	}
	return mid, right
}

// set puts l in the tree, in generation gen, and returns the entry of its
// key that l replaces, if any.
func (t *btree[K, V]) set(gen uint64, l leaf[K, V]) (leaf[K, V], bool) {
	if t.root == nil {
		t.root = newBnode[K, V](gen, 1, 0, 0)
		t.root.items[0] = l
		t.size = 1
		return leaf[K, V]{}, false
	}
	if len(t.root.items) >= maxItems {
		left := t.root.mut(gen, 0)
		mid, right := left.split(maxItems/2, gen)
		t.root = newBnode[K, V](gen, 1, 2, 0)
		t.root.items[0], t.root.kids[0], t.root.kids[1] = mid, left, right
	}
	old, ok := t.insert(gen, l)
	if !ok {
		t.size++
	}
	return old, ok
}

// insert puts l under the root, not full, in generation gen, splitting the
// full nodes on its way down. Each node is made of gen as it is changed,
// at the place that holds it: with room for an entry more if it takes one.
func (t *btree[K, V]) insert(gen uint64, l leaf[K, V]) (leaf[K, V], bool) {
	at := &t.root
	for {
		n := *at
		i, ok := n.find(t.cmp, l.Key)
		switch {
		case ok:
			n = n.mut(gen, 0)
			*at = n
			old := n.items[i]
			n.items[i] = l
			return old, true
		case len(n.kids) == 0:
			n = n.mut(gen, 1)
			*at = n
			n.items = slices.Insert(n.items, i, l)
			return leaf[K, V]{}, false
		case len(n.kids[i].items) >= maxItems:
			n = n.mut(gen, 1)
			*at = n
			mid, right := n.kid(i, gen).split(maxItems/2, gen)
			n.items = slices.Insert(n.items, i, mid)
			n.kids = slices.Insert(n.kids, i+1, right)
			// l goes left of mid, right, or replaces it: in n again.
		default:
			n = n.mut(gen, 0)
			*at = n
			at = &n.kids[i]
		}
	}
}

// del deletes the entry of k, in generation gen, and returns it, if any.
func (t *btree[K, V]) del(gen uint64, k K) (leaf[K, V], bool) {
	if t.root == nil {
		return leaf[K, V]{}, false
	}
	if _, ok := t.get(k); !ok {
		return leaf[K, V]{}, false // copying nothing
	}
	t.root = t.root.mut(gen, 0)
	old, _ := t.root.remove(t.cmp, gen, &k)
	t.size--
	switch {
	case t.size == 0:
		t.root = nil
	case len(t.root.items) == 0:
		t.root = t.root.kids[0]
	}
	return old, true
}

// remove takes the entry of *k out from under n, of generation gen, or,
// if k is nil, the last entry, filling the nodes of minItems on its way
// down so that one can lose an entry.
func (n *bnode[K, V]) remove(cmp func(a, b K) int, gen uint64, k *K) (leaf[K, V], bool) {
	for {
		var i int
		var found bool
		if k == nil {
			i = len(n.items)
			if len(n.kids) == 0 {
				i--
				found = true
			}
		} else {
			i, found = n.find(cmp, *k)
		}
		if len(n.kids) == 0 {
			if !found {
				return leaf[K, V]{}, false
			}
			old := n.items[i]
			n.items = slices.Delete(n.items, i, i+1)
			return old, true
		}
		if len(n.kids[i].items) <= minItems {
			n.grow(i, gen)
			continue // the entry may have moved
		}
		c := n.kid(i, gen)
		if found {
			// Replaced by the last entry before it.
			old := n.items[i]
			n.items[i], _ = c.remove(cmp, gen, nil)
			return old, true
		}
		n = c
	}
}

// grow gives child i of n more than minItems entries: one from a sibling,
// through n, or all of a sibling, merged with it.
func (n *bnode[K, V]) grow(i int, gen uint64) {
	switch {
	case i > 0 && len(n.kids[i-1].items) > minItems:
		n.takeLeft(i, gen)
	case i < len(n.items) && len(n.kids[i+1].items) > minItems:
		n.takeRight(i, gen)
	case i >= len(n.items):
		n.merge(i-1, gen)
	default:
		n.merge(i, gen)
	}
}

// takeLeft gives child i of n the last entry of its left sibling, through
// n: the entry of n between them goes down to it, the sibling's up in
// its place; and the sibling's last child, if any.
func (n *bnode[K, V]) takeLeft(i int, gen uint64) {
	c, left := n.kid(i, gen), n.kid(i-1, gen)
	c.items = slices.Insert(c.items, 0, n.items[i-1])
	last := len(left.items) - 1
	n.items[i-1] = left.items[last]
	left.items[last] = leaf[K, V]{}
	left.items = left.items[:last]
	if len(left.kids) > 0 {
		last := len(left.kids) - 1
		c.kids = slices.Insert(c.kids, 0, left.kids[last])
		left.kids[last] = nil
		left.kids = left.kids[:last]
	}
}

// takeRight gives child i of n the first entry of its right sibling, as
// takeLeft does the last of its left.
func (n *bnode[K, V]) takeRight(i int, gen uint64) {
	c, right := n.kid(i, gen), n.kid(i+1, gen)
	c.items = append(c.items, n.items[i])
	n.items[i] = right.items[0]
	right.items = slices.Delete(right.items, 0, 1)
	if len(right.kids) > 0 {
		c.kids = append(c.kids, right.kids[0])
		right.kids = slices.Delete(right.kids, 0, 1)
	}
}

// merge merges child i+1 of n into child i, with the entry of n between
// them, in a node made for all of them.
func (n *bnode[K, V]) merge(i int, gen uint64) {
	left, right := n.kids[i], n.kids[i+1]
	c := newBnode[K, V](gen, len(left.items)+1+len(right.items), len(left.kids)+len(right.kids), 0)
	c.items = append(append(append(c.items[:0], left.items...), n.items[i]), right.items...)
	c.kids = append(append(c.kids[:0], left.kids...), right.kids...)
	n.kids[i] = c
	n.items = slices.Delete(n.items, i, i+1)
	n.kids = slices.Delete(n.kids, i+1, i+2)
}

// newBnode makes a node of generation gen, of items entries and kids
// children, to fill, with room for more of each. As a write with
// ReadConcurrency copies a node at each level of its path, the node comes
// in one allocation with the room: the least of a few sizes that holds
// them. The least, 8, holds an entry more than minItems, for grow.
func newBnode[K comparable, V any](gen uint64, items, kids, more int) *bnode[K, V] {
	var n *bnode[K, V]
	if kids == 0 {
		n = newBleaf[K, V](items + more)
	} else {
		n = newBinner[K, V](items + more)
	}
	n.gen, n.items, n.kids = gen, n.items[:items], n.kids[:kids]
	return n
}

// newBleaf makes a node of no children with room for room entries, but
// maxItems at most.
func newBleaf[K comparable, V any](room int) *bnode[K, V] {
	switch {
	case room <= 8:
		r := new(bleafRoom8[K, V])
		r.items = r.itemRoom[:]
		return &r.bnode
	case room <= 10:
		r := new(bleafRoom10[K, V])
		r.items = r.itemRoom[:]
		return &r.bnode
	case room <= 12:
		r := new(bleafRoom12[K, V])
		r.items = r.itemRoom[:]
		return &r.bnode
	default:
		r := new(bleafRoom15[K, V])
		r.items = r.itemRoom[:]
		return &r.bnode
	}
}

// newBinner makes a node with room for room entries, but maxItems at
// most, and a child more.
func newBinner[K comparable, V any](room int) *bnode[K, V] {
	switch {
	case room <= 8:
		r := new(binnerRoom8[K, V])
		r.items, r.kids = r.itemRoom[:], r.kidRoom[:]
		return &r.bnode
	case room <= 10:
		r := new(binnerRoom10[K, V])
		r.items, r.kids = r.itemRoom[:], r.kidRoom[:]
		return &r.bnode
	case room <= 12:
		r := new(binnerRoom12[K, V])
		r.items, r.kids = r.itemRoom[:], r.kidRoom[:]
		return &r.bnode
	default:
		r := new(binnerRoom15[K, V])
		r.items, r.kids = r.itemRoom[:], r.kidRoom[:]
		return &r.bnode
	}
}

// A node and the room for its entries, and its children, allocated
// together.
type (
	bleafRoom8[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [8]leaf[K, V]
	}
	bleafRoom10[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [10]leaf[K, V]
	}
	bleafRoom12[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [12]leaf[K, V]
	}
	bleafRoom15[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [maxItems]leaf[K, V]
	}

	binnerRoom8[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [8]leaf[K, V]
		kidRoom  [9]*bnode[K, V]
	}
	binnerRoom10[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [10]leaf[K, V]
		kidRoom  [11]*bnode[K, V]
	}
	binnerRoom12[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [12]leaf[K, V]
		kidRoom  [13]*bnode[K, V]
	}
	binnerRoom15[K comparable, V any] struct {
		bnode[K, V]
		itemRoom [maxItems]leaf[K, V]
		kidRoom  [maxItems + 1]*bnode[K, V]
	}
)

// cursor goes through the entries of a tree in order, from a key on, up
// to an end.
type cursor[K comparable, V any] struct {
	cmp   func(a, b K) int
	hi    K
	hasHi bool
	stack []frame[K, V]
	cur   *leaf[K, V] // the entry at the cursor, nil past the end
}

type frame[K comparable, V any] struct {
	n *bnode[K, V]
	i int // the next entry of n
}

// seek starts c at the first entry of root not before lo, or at the first
// if lo is nil, and stops it before hi, if hasHi.
func (c *cursor[K, V]) seek(root *bnode[K, V], lo *K) {
	c.stack = c.stack[:0]
	for n := root; n != nil; {
		i := 0
		if lo != nil {
			i, _ = n.find(c.cmp, *lo)
		}
		c.stack = append(c.stack, frame[K, V]{n, i})
		if len(n.kids) == 0 {
			break
		}
		n = n.kids[i]
	}
	c.next()
}

// next moves c to the entry after.
func (c *cursor[K, V]) next() {
	for len(c.stack) > 0 {
		f := &c.stack[len(c.stack)-1]
		if f.i == len(f.n.items) {
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}
		l := &f.n.items[f.i]
		f.i++
		if len(f.n.kids) > 0 {
			c.descend(f.n.kids[f.i])
		}
		if c.hasHi && c.cmp(l.Key, c.hi) >= 0 {
			break
		}
		c.cur = l
		return
	}
	c.stack = c.stack[:0]
	c.cur = nil
}

// descend goes down from n to its first leaf, the first entry of each
// node next.
func (c *cursor[K, V]) descend(n *bnode[K, V]) {
	for n != nil {
		c.stack = append(c.stack, frame[K, V]{n, 0})
		if len(n.kids) == 0 {
			return
		}
		n = n.kids[0]
	}
}
