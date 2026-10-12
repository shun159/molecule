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
		if cmp(n.items[m].key, k) < 0 {
			lo = m + 1
		} else {
			hi = m
		}
	}
	return lo, lo < len(n.items) && cmp(n.items[lo].key, k) == 0
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

// mut returns n, or a copy of it if of another generation than gen.
func (n *bnode[K, V]) mut(gen uint64) *bnode[K, V] {
	if n.gen == gen {
		return n
	}
	return &bnode[K, V]{gen: gen, items: slices.Clone(n.items), kids: slices.Clone(n.kids)}
}

func (n *bnode[K, V]) kid(i int, gen uint64) *bnode[K, V] {
	c := n.kids[i].mut(gen)
	n.kids[i] = c
	return c
}

// split cuts n, of generation gen, at its entry i, and returns that entry
// and the node of those after it.
func (n *bnode[K, V]) split(i int, gen uint64) (leaf[K, V], *bnode[K, V]) {
	mid := n.items[i]
	right := &bnode[K, V]{gen: gen, items: slices.Clone(n.items[i+1:])}
	clear(n.items[i:])
	n.items = n.items[:i]
	if len(n.kids) > 0 {
		right.kids = slices.Clone(n.kids[i+1:])
		clear(n.kids[i+1:])
		n.kids = n.kids[:i+1]
	}
	return mid, right
}

// set puts l in the tree, in generation gen, and returns the entry of its
// key that l replaces, if any.
func (t *btree[K, V]) set(gen uint64, l leaf[K, V]) (leaf[K, V], bool) {
	if t.root == nil {
		t.root = &bnode[K, V]{gen: gen, items: []leaf[K, V]{l}}
		t.size = 1
		return leaf[K, V]{}, false
	}
	t.root = t.root.mut(gen)
	if len(t.root.items) >= maxItems {
		mid, right := t.root.split(maxItems/2, gen)
		t.root = &bnode[K, V]{gen: gen, items: []leaf[K, V]{mid}, kids: []*bnode[K, V]{t.root, right}}
	}
	old, ok := t.root.insert(t.cmp, gen, l)
	if !ok {
		t.size++
	}
	return old, ok
}

// insert puts l under n, of generation gen and not full, splitting the
// full nodes on its way down.
func (n *bnode[K, V]) insert(cmp func(a, b K) int, gen uint64, l leaf[K, V]) (leaf[K, V], bool) {
	for {
		i, ok := n.find(cmp, l.key)
		if ok {
			old := n.items[i]
			n.items[i] = l
			return old, true
		}
		if len(n.kids) == 0 {
			n.items = slices.Insert(n.items, i, l)
			return leaf[K, V]{}, false
		}
		if len(n.kids[i].items) >= maxItems {
			mid, right := n.kid(i, gen).split(maxItems/2, gen)
			n.items = slices.Insert(n.items, i, mid)
			n.kids = slices.Insert(n.kids, i+1, right)
			continue // l goes left of mid, right, or replaces it
		}
		n = n.kid(i, gen)
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
	t.root = t.root.mut(gen)
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
	case i < len(n.items) && len(n.kids[i+1].items) > minItems:
		c, right := n.kid(i, gen), n.kid(i+1, gen)
		c.items = append(c.items, n.items[i])
		n.items[i] = right.items[0]
		right.items = slices.Delete(right.items, 0, 1)
		if len(right.kids) > 0 {
			c.kids = append(c.kids, right.kids[0])
			right.kids = slices.Delete(right.kids, 0, 1)
		}
	default:
		if i >= len(n.items) {
			i--
		}
		c, right := n.kid(i, gen), n.kids[i+1]
		c.items = append(c.items, n.items[i])
		c.items = append(c.items, right.items...)
		c.kids = append(c.kids, right.kids...)
		n.items = slices.Delete(n.items, i, i+1)
		n.kids = slices.Delete(n.kids, i+1, i+2)
	}
}

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
			for n := f.n.kids[f.i]; n != nil; {
				c.stack = append(c.stack, frame[K, V]{n, 0})
				if len(n.kids) == 0 {
					break
				}
				n = n.kids[0]
			}
		}
		if c.hasHi && c.cmp(l.key, c.hi) >= 0 {
			break
		}
		c.cur = l
		return
	}
	c.stack = c.stack[:0]
	c.cur = nil
}
