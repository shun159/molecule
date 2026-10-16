package kvs

import (
	"sync"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
)

// An entry of a table with Expires expires at the time Expires tells of
// its value, and is deleted by the first Expire at or after it. Until
// then, it stays: Get and the ranges see it, with its value, which tells
// when it expires.

// Expire deletes the entries expired at now, and returns once the deletes
// are durable, with their number. It panics if the table has no Expires.
func (t *Table[K, V]) Expire(now time.Time) (int, error) {
	n, b, err := t.expire(now)
	if err != nil {
		return n, err
	}
	return n, t.log.wait(b)
}

// NextExpiry returns a time at which an entry may expire, the earliest,
// if any, for a timer to run Expire at: an entry written again since
// may expire later, or be gone.
func (t *Table[K, V]) NextExpiry() (time.Time, bool) {
	t.mustExpire()
	return t.exp.next()
}

// ExpireEffect is the effect deleting the entries expired at now, for a
// behaviour, as Expire does: a Durable with tag arrives once the deletes
// are durable. A behaviour gives the time of its clock, see
// actor.Clocked.
func (t *Table[K, V]) ExpireEffect(now time.Time, tag any) actor.Effect {
	return expireEffect[K, V]{t: t, now: now, tag: tag}
}

type expireEffect[K comparable, V any] struct {
	actor.Extension
	t   *Table[K, V]
	now time.Time
	tag any
}

func (e expireEffect[K, V]) Perform(env gen.Env) {
	n, b, err := e.t.expire(e.now)
	if n > 0 {
		e.t.log.kickRun()
	}
	e.t.deliver(env, e.tag, b, err)
}

func (t *Table[K, V]) mustExpire() {
	if t.exp == nil {
		panic("kvs: expiry of a table without Expires")
	}
}

// expire deletes the entries expired at now, each only if not written
// since it was found expired, and returns their number and the batch of
// the last delete.
func (t *Table[K, V]) expire(now time.Time) (int, *batch, error) {
	t.mustExpire()
	n, last := 0, finished
	for {
		k, ok := t.exp.pop(now.UnixNano())
		if !ok {
			return n, last, nil
		}
		v, rev, ok := t.idx.get(k)
		if !ok {
			continue
		}
		if at := t.opts.Expires(v); at.IsZero() || at.After(now) {
			continue // written again since, to expire later
		}
		b, err := t.delete(k, revIs[V](rev))
		if err == ErrConflict {
			continue // written again since: its expiry is in the heap
		}
		if err != nil {
			return n, nil, err
		}
		n, last = n+1, b
	}
}

// noteExpiry notes that k, just set to v, expires when v does.
func (t *Table[K, V]) noteExpiry(k K, v V) {
	if t.exp == nil {
		return
	}
	if at := t.opts.Expires(v); !at.IsZero() {
		t.exp.push(at.UnixNano(), k)
	}
}

// expiries is a heap of the times entries expire at, the earliest first.
// A write of an entry pushes the time it expires; the times it expired at
// before stay, and are dropped once found out of date.
type expiries[K comparable, V any] struct {
	t     *Table[K, V]
	mu    sync.Mutex
	h     []expiry[K]
	limit int // past which the heap is made again, of the entries
}

type expiry[K comparable] struct {
	at  int64 // unix nanoseconds
	key K
}

// newExpiries makes the heap of the entries of t, as it opens.
func newExpiries[K comparable, V any](t *Table[K, V]) *expiries[K, V] {
	x := &expiries[K, V]{t: t}
	x.rebuild()
	return x
}

func (x *expiries[K, V]) push(at int64, k K) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.h) >= x.limit {
		x.compact() // mostly out of date
	}
	x.h = append(x.h, expiry[K]{at, k})
	x.up(len(x.h) - 1)
}

// pop takes out the earliest time, and its key, if at or before now.
func (x *expiries[K, V]) pop(now int64) (K, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.h) == 0 || x.h[0].at > now {
		var zero K
		return zero, false
	}
	k := x.h[0].key
	last := len(x.h) - 1
	x.h[0] = x.h[last]
	x.h[last] = expiry[K]{}
	x.h = x.h[:last]
	x.down(0)
	return k, true
}

func (x *expiries[K, V]) next() (time.Time, bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.h) == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, x.h[0].at), true
}

// rebuild makes the heap of the times the entries expire at, as the table
// opens.
func (x *expiries[K, V]) rebuild() {
	t := x.t
	clear(x.h)
	x.h = x.h[:0]
	for shard := range t.idx.byShard(false) {
		for l := range shard {
			if at := t.opts.Expires(l.val); !at.IsZero() {
				x.h = append(x.h, expiry[K]{at.UnixNano(), l.key})
			}
		}
	}
	x.heapify()
}

// compact drops the times out of date from the heap, under its lock: of
// entries written again since, or deleted, and a key's but one. It looks
// at the entries of the heap alone, not the table: its cost is of those
// that expire, however many do not.
func (x *expiries[K, V]) compact() {
	seen := make(map[K]struct{}, len(x.h))
	kept := x.h[:0]
	for _, e := range x.h {
		if _, twice := seen[e.key]; twice || !x.current(e) {
			continue
		}
		seen[e.key] = struct{}{}
		kept = append(kept, e)
	}
	clear(x.h[len(kept):])
	x.h = kept
	x.heapify()
}

// current reports whether e is when its entry expires, as written last.
// A write after pushes its time after, once the heap is unlocked.
func (x *expiries[K, V]) current(e expiry[K]) bool {
	v, _, ok := x.t.idx.get(e.key)
	if !ok {
		return false
	}
	at := x.t.opts.Expires(v)
	return !at.IsZero() && at.UnixNano() == e.at
}

// heapify makes a heap of x.h, and sets the limit past which it is
// compacted.
func (x *expiries[K, V]) heapify() {
	for i := len(x.h)/2 - 1; i >= 0; i-- {
		x.down(i)
	}
	x.limit = max(2*len(x.h), 1024)
}

func (x *expiries[K, V]) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if x.h[p].at <= x.h[i].at {
			return
		}
		x.h[p], x.h[i] = x.h[i], x.h[p]
		i = p
	}
}

func (x *expiries[K, V]) down(i int) {
	for {
		m := i
		if l := 2*i + 1; l < len(x.h) && x.h[l].at < x.h[m].at {
			m = l
		}
		if r := 2*i + 2; r < len(x.h) && x.h[r].at < x.h[m].at {
			m = r
		}
		if m == i {
			return
		}
		x.h[i], x.h[m] = x.h[m], x.h[i]
		i = m
	}
}
