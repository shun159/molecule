package kvs

import (
	"errors"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// Ranger is a range of a snapshot of an OrderedSet as a stage: a producer
// emitting the entries of the keys from Lo, up to Hi but not Hi, in order,
// as its consumers ask for them, then done. A client, of this node or
// another, goes over a range of any size so, no more of it in memory, on
// either side, than it asked for, with stage.Pull, say:
//
//	r, err := stage.Start(ctx, node, stage.Produce(kvs.Ranger[K, V]{Snapshot: snap, Lo: lo, Hi: hi}))
//	for e, err := range stage.Pull[kvs.Entry[K, V]](ctx, client, stage.Subscribe{To: r}) {
//
// It is stage.Ephemeral: its consumer gone, it stops. Between nodes, the
// entries go as stage.Register tells.
type Ranger[K comparable, V any] struct {
	Snapshot Snapshot[K, V]
	Lo, Hi   K
}

// Entry is an entry of a table, as a Ranger emits it.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

// ErrRangerOfSet fails the start of a Ranger of a Set, of no order to go
// on from where it was.
var ErrRangerOfSet = errors.New("kvs: Ranger of a Set")

// ranging is where a Ranger is: the key to go on from, after it once it
// has emitted it.
type ranging[K comparable] struct {
	from  K
	after bool
}

func (r Ranger[K, V]) Init(proc.PID) (ranging[K], []actor.Effect, error) {
	if _, ok := r.Snapshot.v.(*treeView[K, V]); !ok {
		return ranging[K]{}, nil, ErrRangerOfSet
	}
	return ranging[K]{from: r.Lo}, nil, nil
}

// HandleDemand emits the next n entries, going on from where it was, and
// is done at the end of the range.
func (r Ranger[K, V]) HandleDemand(at ranging[K], n int) (ranging[K], []actor.Effect) {
	entries := make([]Entry[K, V], 0, min(n, 1024))
	for k, v := range r.Snapshot.Range(at.from, r.Hi) {
		if at.after && k == at.from {
			continue // emitted already
		}
		if len(entries) == n {
			break
		}
		entries = append(entries, Entry[K, V]{Key: k, Value: v})
	}
	out := actor.Do(stage.Emit[Entry[K, V]]{Events: entries})
	if len(entries) < n {
		return at, append(out, stage.Done{})
	}
	return ranging[K]{from: entries[n-1].Key, after: true}, out
}

// StopAlone has a Ranger stop once its consumer is gone.
func (Ranger[K, V]) StopAlone() bool { return true }
