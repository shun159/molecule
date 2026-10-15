package pmap

import "testing"

// Keys of one hash, as two of 64 bits seldom are, share a node past the
// bits of the hash, and are found, replaced and deleted there.
func TestCollisions(t *testing.T) {
	root := &node[string, int]{}
	for i, k := range []string{"a", "b", "c"} {
		root, _ = root.set(0, entry[string, int]{hash: 42, key: k, val: i})
	}
	root, replaced := root.set(0, entry[string, int]{hash: 42, key: "b", val: 9})
	if e, ok := root.get(42, "b"); !replaced || !ok || e.val != 9 {
		t.Fatalf("b replaced %v: %+v, %v", replaced, e, ok)
	}
	root, deleted := root.del(0, 42, "a")
	if _, ok := root.get(42, "a"); !deleted || ok {
		t.Fatalf("a deleted %v, found %v", deleted, ok)
	}
	root, _ = root.del(0, 42, "c")
	if e, ok := root.get(42, "b"); !ok || e.val != 9 || len(root.data) != 1 || len(root.nodes) != 0 {
		t.Errorf("b alone left: %+v, %v; root %d entries, %d nodes", e, ok, len(root.data), len(root.nodes))
	}
}
