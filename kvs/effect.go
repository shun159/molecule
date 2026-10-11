package kvs

import (
	"context"

	"github.com/shun159/molecule"
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
//	return s, molecule.Do(s.table.PutEffect(mac, lease, from))
//	...
//	case kvs.Durable:
//		return s, molecule.Do(molecule.Reply{To: m.Tag.(molecule.From), Value: ack})
func (t *Table[K, V]) PutEffect(k K, v V, tag any) molecule.Effect {
	return writeEffect[K, V]{t: t, key: k, val: v, tag: tag}
}

// DeleteEffect is the effect deleting the entry under k, as PutEffect is
// that putting one.
func (t *Table[K, V]) DeleteEffect(k K, tag any) molecule.Effect {
	return writeEffect[K, V]{t: t, del: true, key: k, tag: tag}
}

type writeEffect[K comparable, V any] struct {
	molecule.Extension
	t   *Table[K, V]
	del bool
	key K
	val V
	tag any
}

func (e writeEffect[K, V]) Perform(env gen.Env) {
	var b *batch
	var err error
	if e.del {
		b, err = e.t.delete(e.key)
	} else {
		b, err = e.t.put(e.key, e.val)
	}
	if err == nil {
		e.t.log.kickRun()
	}
	tag := e.tag
	if tag == nil {
		if err != nil {
			env.Logger().Error("kvs: write failed", "dir", e.t.dir, "error", err)
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
