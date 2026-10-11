package kvs

import (
	"math/rand/v2"
	"testing"
)

// TestTrie checks the trie against a map, with hashes of few bits so that
// entries share paths and whole hashes.
func TestTrie(t *testing.T) {
	for _, mask := range []uint64{^uint64(0), 0xff, 0x3, 0xf000_0000_0000_000f} {
		r := rand.New(rand.NewPCG(1, mask))
		root := &hnode[int, int]{}
		want := map[int]int{}
		hash := func(k int) uint64 { return uint64(k) * 0x9e3779b97f4a7c15 & mask }
		for i := range 20000 {
			k := r.IntN(500)
			if r.IntN(3) == 0 {
				n, l, ok := root.del(0, hash(k), k)
				_, had := want[k]
				if ok != had || (ok && l.key != k) {
					t.Fatalf("mask %x step %d: del %d: %v, want %v", mask, i, k, ok, had)
				}
				delete(want, k)
				root = n
			} else {
				n, l, ok := root.set(0, leaf[int, int]{hash: hash(k), key: k, val: i})
				v, had := want[k]
				if ok != had || (ok && l.val != v) {
					t.Fatalf("mask %x step %d: set %d replaced %v %d, want %v %d", mask, i, k, ok, l.val, had, v)
				}
				want[k] = i
				root = n
			}
			if i%997 == 0 {
				check(t, root, want, hash)
			}
		}
		check(t, root, want, hash)

		// An old root still reads as it was.
		snap := root
		snapWant := map[int]int{}
		for k, v := range want {
			snapWant[k] = v
		}
		for k := range 500 {
			root, _, _ = root.set(0, leaf[int, int]{hash: hash(k), key: k, val: -1})
		}
		check(t, snap, snapWant, hash)
	}
}

func check(t *testing.T, root *hnode[int, int], want map[int]int, hash func(int) uint64) {
	t.Helper()
	n := 0
	root.each(func(l *leaf[int, int]) bool {
		n++
		if v, ok := want[l.key]; !ok || v != l.val {
			t.Fatalf("entry %d=%d, want %d %v", l.key, l.val, v, ok)
		}
		return true
	})
	if n != len(want) {
		t.Fatalf("%d entries, want %d", n, len(want))
	}
	for k, v := range want {
		if l, ok := root.get(hash(k), k); !ok || l.val != v {
			t.Fatalf("get %d: %v, want %d", k, ok, v)
		}
	}
	canonical(t, root, true)
}

// canonical checks that no node below the root holds a single entry.
func canonical(t *testing.T, n *hnode[int, int], top bool) {
	t.Helper()
	if !top && len(n.data) == 1 && len(n.nodes) == 0 {
		t.Fatal("a node of a single entry below the root")
	}
	for _, c := range n.nodes {
		canonical(t, c, false)
	}
}

// TestTrieInPlace builds a trie in place, then goes on with it as usual.
func TestTrieInPlace(t *testing.T) {
	for _, mask := range []uint64{^uint64(0), 0xff, 0x3} {
		hash := func(k int) uint64 { return uint64(k) * 0x9e3779b97f4a7c15 & mask }
		root := &hnode[int, int]{}
		want := map[int]int{}
		for i := range 3000 {
			k := i % 700
			_, replaced := root.setInPlace(0, leaf[int, int]{hash: hash(k), key: k, val: i})
			if _, had := want[k]; replaced != had {
				t.Fatalf("mask %x: setInPlace %d replaced %v", mask, k, replaced)
			}
			want[k] = i
		}
		check(t, root, want, hash)
		built := root
		for k := range 350 {
			root, _, _ = root.del(0, hash(k), k)
		}
		check(t, built, want, hash) // unchanged by the deletes
	}
}
