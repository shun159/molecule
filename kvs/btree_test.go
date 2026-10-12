package kvs

import (
	"cmp"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestBTree checks the tree against a map, in place and copied on write,
// and that a frozen version reads as it was.
func TestBTree(t *testing.T) {
	for _, cow := range []bool{false, true} {
		r := rand.New(rand.NewPCG(1, 2))
		tr := btree[int, int]{cmp: cmp.Compare[int]}
		want := map[int]int{}
		gen := uint64(1)
		var frozen *bnode[int, int]
		var frozenWant map[int]int
		for i := range 30000 {
			if cow {
				gen++
			}
			k := r.IntN(2000)
			if r.IntN(3) == 0 {
				l, ok := tr.del(gen, k)
				v, had := want[k]
				if ok != had || ok && l.val != v {
					t.Fatalf("cow %v step %d: del %d: %v %d, want %v %d", cow, i, k, ok, l.val, had, v)
				}
				delete(want, k)
			} else {
				l, ok := tr.set(gen, leaf[int, int]{key: k, val: i})
				v, had := want[k]
				if ok != had || ok && l.val != v {
					t.Fatalf("cow %v step %d: set %d replaced %v %d, want %v %d", cow, i, k, ok, l.val, had, v)
				}
				want[k] = i
			}
			if i%1009 == 0 {
				checkTree(t, tr.root, tr.size, want)
			}
			if i == 15000 {
				// Freeze: the writes after take a new generation.
				frozen, frozenWant = tr.root, maps.Clone(want)
				gen++
			}
		}
		checkTree(t, tr.root, tr.size, want)
		checkTree(t, frozen, len(frozenWant), frozenWant)

		// All deleted, the tree is empty.
		for k := range want {
			tr.del(gen, k)
		}
		if tr.root != nil || tr.size != 0 {
			t.Fatalf("cow %v: emptied, %d entries left", cow, tr.size)
		}
	}
}

// checkTree checks the tree under root holds want, in order, its nodes
// filled as they ought to be and its leaves at one depth, and that a
// cursor ranges over any part of it.
func checkTree(t *testing.T, root *bnode[int, int], size int, want map[int]int) {
	t.Helper()
	if size != len(want) {
		t.Fatalf("size %d, want %d", size, len(want))
	}
	var got []int
	depth := -1
	var walk func(n *bnode[int, int], d int)
	walk = func(n *bnode[int, int], d int) {
		if len(n.items) > maxItems || n != root && len(n.items) < minItems {
			t.Fatalf("a node of %d entries", len(n.items))
		}
		if len(n.kids) == 0 {
			if depth >= 0 && depth != d {
				t.Fatalf("leaves at depths %d and %d", depth, d)
			}
			depth = d
			for _, l := range n.items {
				got = append(got, l.key)
			}
			return
		}
		if len(n.kids) != len(n.items)+1 {
			t.Fatalf("a node of %d entries and %d children", len(n.items), len(n.kids))
		}
		for i, c := range n.kids {
			walk(c, d+1)
			if i < len(n.items) {
				got = append(got, n.items[i].key)
			}
		}
	}
	if root != nil {
		walk(root, 0)
	}
	keys := slices.Sorted(maps.Keys(want))
	if !slices.Equal(got, keys) {
		t.Fatalf("keys %v, want %v", got, keys)
	}
	tr := btree[int, int]{cmp: cmp.Compare[int], root: root}
	for _, k := range keys {
		if l, ok := tr.get(k); !ok || l.val != want[k] {
			t.Fatalf("get %d: %v", k, ok)
		}
	}
	for _, b := range [][2]int{{-1, 5000}, {0, 0}, {100, 101}, {500, 1500}, {1999, 3000}} {
		c := cursor[int, int]{cmp: cmp.Compare[int], hi: b[1], hasHi: true}
		lo := b[0]
		var got []int
		for c.seek(root, &lo); c.cur != nil; c.next() {
			got = append(got, c.cur.key)
		}
		var want []int
		for _, k := range keys {
			if k >= b[0] && k < b[1] {
				want = append(want, k)
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("range %v: %v, want %v", b, got, want)
		}
	}
}
