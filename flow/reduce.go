package flow

import (
	"time"

	"github.com/shun159/molecule/pmap"
)

// reducer is the reduce of a layer, the type of its accumulator erased:
// fold has the operations before it, then the add, fold the events of the
// layer into it; flush gives what it is, and the operations after the
// reduce make of it, when it is let out, at the end, or at the end of a
// window of every.
type reducer[S, T any] struct {
	init  func() any
	fold  func(acc any, events []S) any
	flush func(acc any, start time.Time, emit func(T))
	every time.Duration
}

// Window is what ReduceEvery lets out of a window: when it started, and
// the accumulator of its events.
type Window[A any] struct {
	Start time.Time
	Acc   A
}

// KV is a key, and what ReduceByKey made of its events.
type KV[K comparable, V any] struct {
	Key   K
	Value V
}

// Reduce folds the events of f, each worker of its layer its own, into
// an accumulator, starting at init, the next made by add of the last and
// an event; and lets each out once the sources are done. After a
// Partition, of a key, the events of each worker are those of its keys.
// The accumulator is part of the state of a worker: add is not to write
// the one it is given, a map, say, but return another; a pmap.Map costs
// little so.
//
// A layer reduces once: another Reduce wants a Partition before it, or
// it panics. The operations after a Reduce are on what it lets out.
func Reduce[S, T, A any](f Flow[S, T], init func() A, add func(A, T) A) Flow[S, A] {
	return reduce(f, 0, init, add, func(a A, _ time.Time, emit func(A)) { emit(a) })
}

// ReduceEvery is Reduce letting out the accumulator of each window of
// every, and starting afresh, as stage.Windowing cuts time: those of
// windows of processing time, of the multiples of every; at the end, that
// of the window then.
func ReduceEvery[S, T, A any](f Flow[S, T], every time.Duration, init func() A, add func(A, T) A) Flow[S, Window[A]] {
	return reduce(f, every, init, add, func(a A, start time.Time, emit func(Window[A])) {
		emit(Window[A]{Start: start, Acc: a})
	})
}

// ReduceByKey is Reduce by key: the events of each key folded, starting
// at init, and, at the end, one KV for each key, in no order.
func ReduceByKey[S, T any, K comparable, V any](f Flow[S, T], key func(T) K, init func() V, add func(V, T) V) Flow[S, KV[K, V]] {
	if f.red != nil {
		panic("flow: a Reduce of a layer reduced: Partition before it")
	}
	return Flow[S, KV[K, V]]{from: f.from, done: f.done, stages: f.stages, red: &reducer[S, KV[K, V]]{
		init: func() any { return pmap.Map[K, V]{} },
		fold: func(acc any, events []S) any {
			return foldByKey(acc.(pmap.Map[K, V]), events, f.xf, key, init, add)
		},
		flush: func(acc any, _ time.Time, emit func(KV[K, V])) {
			for k, v := range acc.(pmap.Map[K, V]).All() {
				emit(KV[K, V]{Key: k, Value: v})
			}
		},
	}}
}

// foldByKey folds the events into m by key: those of the batch first in a
// map of its own, then into m, a key at a time, rather than an event.
func foldByKey[S, T any, K comparable, V any](m pmap.Map[K, V], events []S, xf func(func(T)) func(S),
	key func(T) K, init func() V, add func(V, T) V) pmap.Map[K, V] {
	batch := make(map[K]V)
	push := xf(func(t T) {
		k := key(t)
		v, ok := batch[k]
		if !ok {
			if v, ok = m.Get(k); !ok {
				v = init()
			}
		}
		batch[k] = add(v, t)
	})
	for _, e := range events {
		push(e)
	}
	for k, v := range batch {
		m = m.Set(k, v)
	}
	return m
}

// reduce ends the operations of the layer of f by a reduce into A, which
// flush lets out as events of U.
func reduce[S, T, A, U any](f Flow[S, T], every time.Duration, init func() A, add func(A, T) A,
	flush func(a A, start time.Time, emit func(U))) Flow[S, U] {
	if f.red != nil {
		panic("flow: a Reduce of a layer reduced: Partition before it")
	}
	xf := f.xf
	return Flow[S, U]{from: f.from, done: f.done, stages: f.stages, red: &reducer[S, U]{
		init: func() any { return init() },
		fold: func(acc any, events []S) any {
			a := acc.(A)
			push := xf(func(t T) { a = add(a, t) })
			for _, e := range events {
				push(e)
			}
			return a
		},
		flush: func(acc any, start time.Time, emit func(U)) { flush(acc.(A), start, emit) },
		every: every,
	}}
}
