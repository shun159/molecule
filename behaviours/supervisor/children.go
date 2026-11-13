package supervisor

import (
	"cmp"
	"slices"

	"github.com/shun159/molecule/pmap"
	"github.com/shun159/molecule/proc"
)

// children are the children of a supervisor by key, and the keys of those
// running by PID: in maps never changed, so that a state with them is
// changed at the cost of a few nodes, not of a copy of them all, as a
// state a callback got must stay as it was.
//
// Keys only grow, so the children in the order of their keys are in the
// order they were added. pmap lists them in no order: ordered sorts them,
// for a supervisor to act the same on the same children, in gensim too.
type children struct {
	byKey pmap.Map[uint64, child]
	byPID pmap.Map[proc.PID, uint64]
}

func (cs children) len() int { return cs.byKey.Len() }

// get returns the child key, if any.
func (cs children) get(key uint64) (child, bool) { return cs.byKey.Get(key) }

// keyOf returns the key of the child running as pid, if any.
func (cs children) keyOf(pid proc.PID) (uint64, bool) {
	if pid.IsZero() {
		return 0, false // children not running have a zero PID
	}
	return cs.byPID.Get(pid)
}

// set returns cs with c in place of the child of its key, if any.
func (cs children) set(c child) children {
	if old, ok := cs.byKey.Get(c.key); ok && old.pid != c.pid && !old.pid.IsZero() {
		cs.byPID = cs.byPID.Delete(old.pid)
	}
	if !c.pid.IsZero() {
		cs.byPID = cs.byPID.Set(c.pid, c.key)
	}
	cs.byKey = cs.byKey.Set(c.key, c)
	return cs
}

// delete returns cs without the child key.
func (cs children) delete(key uint64) children {
	if c, ok := cs.byKey.Get(key); ok && !c.pid.IsZero() {
		cs.byPID = cs.byPID.Delete(c.pid)
	}
	cs.byKey = cs.byKey.Delete(key)
	return cs
}

// ordered returns the children in the order they were added.
func (cs children) ordered() []child {
	all := make([]child, 0, cs.len())
	for _, c := range cs.byKey.All() {
		all = append(all, c)
	}
	slices.SortFunc(all, func(a, b child) int { return cmp.Compare(a.key, b.key) })
	return all
}
