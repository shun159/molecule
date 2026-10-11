package kvs

import (
	"errors"

	"github.com/shun159/molecule"
)

// ErrConflict tells a behaviour, in a Durable, that the entry of its
// conditional write was not as the write required: nothing was written.
var ErrConflict = errors.New("kvs: conflict")

// An entry has a revision: the seq of the write that set it, which grows
// with each write to the table. A conditional write compares it, as etcd
// compares the mod revision of a key: it writes only if the entry is still
// the one read, and does not depend on V being comparable.

// GetRev returns the value under k and its revision.
func (t *Table[K, V]) GetRev(k K) (V, uint64, bool) { return t.idx.get(k) }

// PutIf puts v under k if the revision of the entry under k is rev, or,
// with rev 0, if there is none, and returns once the write is durable. It
// reports whether it wrote.
func (t *Table[K, V]) PutIf(k K, v V, rev uint64) (bool, error) {
	return t.wait(t.put(k, v, revIs[V](rev)))
}

// DeleteIf deletes the entry under k if its revision is rev, and returns
// once the delete is durable. It reports whether it deleted.
func (t *Table[K, V]) DeleteIf(k K, rev uint64) (bool, error) {
	return t.wait(t.delete(k, revIs[V](rev)))
}

// CompareAndSwap puts new under k if the value under k is old, as
// sync.Map.CompareAndSwap does, and returns once the write is durable. It
// reports whether it wrote.
func CompareAndSwap[K, V comparable](t *Table[K, V], k K, old, new V) (bool, error) {
	return t.wait(t.put(k, new, valueIs(old)))
}

// CompareAndDelete deletes the entry under k if its value is old, as
// sync.Map.CompareAndDelete does, and returns once the delete is durable.
// It reports whether it deleted.
func CompareAndDelete[K, V comparable](t *Table[K, V], k K, old V) (bool, error) {
	return t.wait(t.delete(k, valueIs(old)))
}

// PutIfEffect is the effect putting v under k as PutIf does, for a
// behaviour, as PutEffect is that putting it: a Durable with ErrConflict
// arrives if it did not write.
func (t *Table[K, V]) PutIfEffect(k K, v V, rev uint64, tag any) molecule.Effect {
	return writeEffect[K, V]{t: t, key: k, val: v, tag: tag, ifRev: true, rev: rev}
}

// DeleteIfEffect is the effect deleting the entry under k as DeleteIf
// does, as PutIfEffect is that putting one.
func (t *Table[K, V]) DeleteIfEffect(k K, rev uint64, tag any) molecule.Effect {
	return writeEffect[K, V]{t: t, del: true, key: k, tag: tag, ifRev: true, rev: rev}
}

func revIs[V any](want uint64) matcher[V] {
	return func(_ V, rev uint64, ok bool) bool { return ok && rev == want || !ok && want == 0 }
}

func valueIs[V comparable](want V) matcher[V] {
	return func(v V, _ uint64, ok bool) bool { return ok && v == want }
}

// wait waits for the write of b, if made, and reports whether it was.
func (t *Table[K, V]) wait(b *batch, err error) (bool, error) {
	if err == ErrConflict {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, t.log.wait(b)
}
