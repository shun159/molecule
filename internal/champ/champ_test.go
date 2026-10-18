package champ

import (
	"math/rand/v2"
	"testing"
)

// TestTrie checks the trie against a map, with hashes of few bits so that
// entries share paths and whole hashes.
func TestTrie(t *testing.T) {
	for _, mask := range []uint64{^uint64(0), 0xff, 0x3, 0xf000_0000_0000_000f} {
		r := rand.New(rand.NewPCG(1, mask))
		root := &Node[int, int, struct{}]{}
		want := map[int]int{}
		hash := func(k int) uint64 { return uint64(k) * 0x9e3779b97f4a7c15 & mask }
		for i := range 20000 {
			k := r.IntN(500)
			if r.IntN(3) == 0 {
				n, l, ok := root.Delete(hash(k), k)
				_, had := want[k]
				if ok != had || (ok && l.Key != k) {
					t.Fatalf("mask %x step %d: del %d: %v, want %v", mask, i, k, ok, had)
				}
				delete(want, k)
				root = n
			} else {
				n, l, ok := root.Set(Entry[int, int, struct{}]{Hash: hash(k), Key: k, Val: i})
				v, had := want[k]
				if ok != had || (ok && l.Val != v) {
					t.Fatalf("mask %x step %d: set %d replaced %v %d, want %v %d", mask, i, k, ok, l.Val, had, v)
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
			root, _, _ = root.Set(Entry[int, int, struct{}]{Hash: hash(k), Key: k, Val: -1})
		}
		check(t, snap, snapWant, hash)
	}
}

func check(t *testing.T, root *Node[int, int, struct{}], want map[int]int, hash func(int) uint64) {
	t.Helper()
	n := 0
	root.Each(func(l *Entry[int, int, struct{}]) bool {
		n++
		if v, ok := want[l.Key]; !ok || v != l.Val {
			t.Fatalf("entry %d=%d, want %d %v", l.Key, l.Val, v, ok)
		}
		return true
	})
	if n != len(want) {
		t.Fatalf("%d entries, want %d", n, len(want))
	}
	for k, v := range want {
		if l, ok := root.Get(hash(k), k); !ok || l.Val != v {
			t.Fatalf("get %d: %v, want %d", k, ok, v)
		}
	}
	canonical(t, root, true)
}

// canonical checks that no node below the root holds a single entry.
func canonical(t *testing.T, n *Node[int, int, struct{}], top bool) {
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
		root := &Node[int, int, struct{}]{}
		want := map[int]int{}
		for i := range 3000 {
			k := i % 700
			_, replaced := root.SetInPlace(Entry[int, int, struct{}]{Hash: hash(k), Key: k, Val: i})
			if _, had := want[k]; replaced != had {
				t.Fatalf("mask %x: setInPlace %d replaced %v", mask, k, replaced)
			}
			want[k] = i
		}
		check(t, root, want, hash)
		built := root
		for k := range 350 {
			root, _, _ = root.Delete(hash(k), k)
		}
		check(t, built, want, hash) // unchanged by the deletes
	}
}

// Keys of one hash, as two of 64 bits seldom are, share a node past the
// bits of the hash, and are found, replaced and deleted there.
func TestCollisions(t *testing.T) {
	type entry = Entry[string, int, struct{}]
	root := &Node[string, int, struct{}]{}
	for i, k := range []string{"a", "b", "c"} {
		root, _, _ = root.Set(entry{Hash: 42, Key: k, Val: i})
	}
	root, _, replaced := root.Set(entry{Hash: 42, Key: "b", Val: 9})
	if e, ok := root.Get(42, "b"); !replaced || !ok || e.Val != 9 {
		t.Fatalf("b replaced %v: %+v, %v", replaced, e, ok)
	}
	root, _, deleted := root.Delete(42, "a")
	if _, ok := root.Get(42, "a"); !deleted || ok {
		t.Fatalf("a deleted %v, found %v", deleted, ok)
	}
	root, _, _ = root.Delete(42, "c")
	if e, ok := root.Get(42, "b"); !ok || e.Val != 9 || len(root.data) != 1 || len(root.nodes) != 0 {
		t.Errorf("b alone left: %+v, %v; root %d entries, %d nodes", e, ok, len(root.data), len(root.nodes))
	}
}
