package kvs

import (
	"maps"
	"slices"
	"testing"
)

// A dirOp is a change to the names of a directory since its last sync,
// which a power loss keeps whole, or loses whole: a file created, or
// removed, or renamed, onto a name free or taken. It holds each name it
// changes, with the inode the name had at the last sync, and the inode it
// has now; 0 for none.
type dirOp []nameChange

type nameChange struct {
	name     string
	was, now uint64
}

// applyTo makes names, those of the last sync, those of op kept.
func (op dirOp) applyTo(names map[string]uint64) {
	for _, c := range op {
		if c.now == 0 {
			delete(names, c.name)
		} else {
			names[c.name] = c.now
		}
	}
}

// dirOps returns the changes from was, the names of a directory at its
// last sync, by inode, to now, its names now: each name changed in one of
// them, and those changed together in one.
//
// A rename of a file from a name to another changes both, the one losing
// the inode, the other taking it: one change, no power loss keeping the
// file under neither name, nor both. A rename onto a name taken removes
// the file it had: one change as well. A name changed by two changes, as
// a file renamed onto a name another file was renamed from, makes them
// one. A file created, or removed, alone, is a change of its own.
//
// The changes come in the order of their first names, for a test picking
// some by their index to pick the same again.
func dirOps(was, now map[string]uint64) []dirOp {
	changed := changedNames(was, now)
	// Names joined by an inode they both had, or have: one change.
	parent := make(map[string]string, len(changed))
	var find func(n string) string
	find = func(n string) string {
		if parent[n] == n {
			return n
		}
		parent[n] = find(parent[n])
		return parent[n]
	}
	byIno := map[uint64]string{} // a name changed that had, or has, the inode
	for _, n := range changed {
		parent[n] = n
		for _, ino := range []uint64{was[n], now[n]} {
			if ino == 0 {
				continue
			}
			if m, ok := byIno[ino]; ok {
				parent[find(n)] = find(m)
			} else {
				byIno[ino] = n
			}
		}
	}
	var ops []dirOp
	at := map[string]int{} // the index in ops of the change of each root
	for _, n := range changed {
		root := find(n)
		i, ok := at[root]
		if !ok {
			i = len(ops)
			at[root] = i
			ops = append(ops, nil)
		}
		ops[i] = append(ops[i], nameChange{name: n, was: was[n], now: now[n]})
	}
	return ops
}

// changedNames returns the names of was or of now whose inode is not the
// same in both, in order.
func changedNames(was, now map[string]uint64) []string {
	var changed []string
	for n, ino := range was {
		if now[n] != ino {
			changed = append(changed, n)
		}
	}
	for n := range now {
		if _, ok := was[n]; !ok {
			changed = append(changed, n)
		}
	}
	slices.Sort(changed)
	return changed
}

// TestDirOps tells the changes of a directory, as a power loss keeps or
// loses them.
func TestDirOps(t *testing.T) {
	cases := []struct {
		name     string
		was, now map[string]uint64
		want     [][]nameChange // in no order, nor their changes
	}{
		{"none", map[string]uint64{"a": 1}, map[string]uint64{"a": 1}, nil},
		{"create", map[string]uint64{"a": 1}, map[string]uint64{"a": 1, "b": 2},
			[][]nameChange{{{"b", 0, 2}}}},
		{"remove", map[string]uint64{"a": 1, "b": 2}, map[string]uint64{"a": 1},
			[][]nameChange{{{"b", 2, 0}}}},
		{"rename", map[string]uint64{"a.tmp": 1}, map[string]uint64{"a": 1},
			[][]nameChange{{{"a.tmp", 1, 0}, {"a", 0, 1}}}},
		{"rename onto a name taken", map[string]uint64{"a.tmp": 1, "a": 2}, map[string]uint64{"a": 1},
			[][]nameChange{{{"a.tmp", 1, 0}, {"a", 2, 1}}}},
		{"two renames through a name", map[string]uint64{"a": 1, "b": 2}, map[string]uint64{"b": 1, "c": 2},
			[][]nameChange{{{"a", 1, 0}, {"b", 2, 1}, {"c", 0, 2}}}},
		{"a snapshot in, segments out", map[string]uint64{"seg1": 1, "seg2": 2, "snap.tmp": 3, "seg3": 4},
			map[string]uint64{"snap": 3, "seg3": 4},
			[][]nameChange{{{"snap.tmp", 3, 0}, {"snap", 0, 3}}, {{"seg1", 1, 0}}, {{"seg2", 2, 0}}}},
		{"created, then renamed", map[string]uint64{}, map[string]uint64{"a": 5},
			[][]nameChange{{{"a", 0, 5}}}},
	}
	for _, c := range cases {
		got := dirOps(c.was, c.now)
		if !sameOps(got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
		all := maps.Clone(c.was)
		for _, op := range got {
			op.applyTo(all)
		}
		if !maps.Equal(all, c.now) {
			t.Errorf("%s: all kept, the names are %v, not %v", c.name, all, c.now)
		}
	}
}

// sameOps reports whether got are the changes of want, in any order.
func sameOps(got []dirOp, want [][]nameChange) bool {
	key := func(cs []nameChange) string {
		cs = slices.Clone(cs)
		slices.SortFunc(cs, func(a, b nameChange) int {
			if a.name < b.name {
				return -1
			}
			if a.name > b.name {
				return 1
			}
			return 0
		})
		s := ""
		for _, c := range cs {
			s += c.name + ":" + string(rune('0'+c.was)) + ">" + string(rune('0'+c.now)) + " "
		}
		return s
	}
	var gs, ws []string
	for _, op := range got {
		gs = append(gs, key(op))
	}
	for _, cs := range want {
		ws = append(ws, key(cs))
	}
	slices.Sort(gs)
	slices.Sort(ws)
	return slices.Equal(gs, ws)
}

// keepings are the ways a test keeps k changes not synced at a power
// loss: none, all, all but each, and each alone. All but one tells a
// change lost with those after it kept, the order a sync of the
// directory is to keep.
func keepings(k int) []func(int) bool {
	ks := []func(int) bool{
		func(int) bool { return false },
	}
	if k == 0 {
		return ks
	}
	ks = append(ks, func(int) bool { return true })
	for j := range k {
		ks = append(ks, func(i int) bool { return i != j }, func(i int) bool { return i == j })
	}
	return ks
}
