package kvs

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
)

// Durable tells a behaviour that the write of its effect under Tag is
// durable, or failed with Err. The Durables of writes may arrive in any
// order; a write durable, those before it are too.
type Durable struct {
	Tag any
	Err error
}

// PutEffect is the effect putting v under k, for a behaviour: the runtime
// of the behaviour puts it, as Put does, without waiting, and a Durable
// with tag arrives once the write is durable. With a nil tag, none does;
// a failure is logged.
//
//	s.leases[mac] = lease
//	return s, actor.Do(s.table.PutEffect(mac, lease, from))
//	...
//	case kvs.Durable:
//		return s, actor.Do(actor.Reply{To: m.Tag.(actor.From), Value: ack})
func (t *Table[K, V]) PutEffect(k K, v V, tag any) actor.Effect {
	return writeEffect[K, V]{t: t, key: k, val: v, tag: tag}
}

// DeleteEffect is the effect deleting the entry under k, as PutEffect is
// that putting one.
func (t *Table[K, V]) DeleteEffect(k K, tag any) actor.Effect {
	return writeEffect[K, V]{t: t, del: true, key: k, tag: tag}
}

type writeEffect[K comparable, V any] struct {
	actor.Extension
	t     *Table[K, V]
	del   bool
	key   K
	val   V
	tag   any
	ifRev bool // written only if the revision of the entry is rev
	rev   uint64
}

func (e writeEffect[K, V]) Perform(env gen.Env) {
	var b *batch
	var err error
	var match matcher[V]
	if e.ifRev {
		match = revIs[V](e.rev)
	}
	if e.del {
		b, err = e.t.delete(e.key, match)
	} else {
		b, err = e.t.put(e.key, e.val, match)
	}
	if err == nil {
		e.t.log.kickRun()
	}
	e.t.deliver(env, e.tag, b, err)
}

// Write is a put of Val under Key, or the delete of Key, for WritesEffect.
type Write[K comparable, V any] struct {
	Key    K
	Val    V
	Delete bool
}

// WritesEffect is the effect making writes in order, as PutEffect and
// DeleteEffect make one, all or none if one fails to encode: then none is
// made. One Durable with tag arrives, once the last is durable, those
// before it with it, or with the error of the write that failed.
//
//	actor.Do(t.WritesEffect(from, kvs.Write[K, V]{Key: k, Val: v}, kvs.Write[K, V]{Key: old, Delete: true}))
func (t *Table[K, V]) WritesEffect(tag any, writes ...Write[K, V]) actor.Effect {
	return writesEffect[K, V]{t: t, writes: writes, tag: tag}
}

type writesEffect[K comparable, V any] struct {
	actor.Extension
	t      *Table[K, V]
	writes []Write[K, V]
	tag    any
}

func (e writesEffect[K, V]) Perform(env gen.Env) {
	if len(e.writes) == 0 {
		if e.tag != nil {
			env.Send(env.Self(), Durable{Tag: e.tag})
		}
		return
	}
	b, err := e.t.writes(e.writes)
	e.t.deliver(env, e.tag, b, err)
}

// Writes makes writes as WritesEffect does, and returns once they are
// durable: for a goroutine of its own, not a behaviour.
func (t *Table[K, V]) Writes(writes ...Write[K, V]) error {
	if len(writes) == 0 {
		return nil
	}
	b, err := t.writes(writes)
	if err != nil {
		return err
	}
	return t.log.wait(b)
}

// writes makes writes, all or none if one fails to encode, and returns the
// batch of the last.
func (t *Table[K, V]) writes(writes []Write[K, V]) (*batch, error) {
	recs := make([]rec, len(writes))
	for i, w := range writes {
		op := opPut
		if w.Delete {
			op = opDel
		}
		var buf []byte
		body, err := t.encode(&buf, op, w.Key, w.Val)
		if err != nil {
			return nil, err
		}
		recs[i] = newRec(body)
	}
	var b *batch
	var err error
	for i, w := range writes {
		if w.Delete {
			b, err = t.idx.del(t.log, w.Key, recs[i], nil)
		} else if b, err = t.idx.put(t.log, w.Key, w.Val, recs[i], nil); err == nil {
			t.noteExpiry(w.Key, w.Val)
		}
		if err != nil {
			return nil, err
		}
	}
	t.maybeCompact()
	t.log.kickRun()
	return b, nil
}

// deliver sends the behaviour of env a Durable with tag once b is
// durable, or with err at once, if any; with a nil tag, it logs err.
func (t *Table[K, V]) deliver(env gen.Env, tag any, b *batch, err error) {
	if tag == nil {
		if err != nil && err != ErrConflict {
			env.Logger().Error("kvs: write failed", "dir", t.dir, "error", err)
		}
		return
	}
	if err != nil {
		env.Send(env.Self(), Durable{Tag: tag, Err: err})
		return
	}
	select {
	case <-b.done:
		env.Send(env.Self(), Durable{Tag: tag, Err: b.err})
		return
	default:
	}
	env.Async(func(ctx context.Context) (any, error) {
		select {
		case <-b.done:
			return nil, b.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, func(_ any, err error) any { return Durable{Tag: tag, Err: err} })
}
