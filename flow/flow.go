package flow

import (
	"hash/maphash"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
)

// Flow is a plan of work: the events of its sources made events of type
// T by the operations fused on them, in layers of workers, a Partition
// starting the next; S is the type of the events coming into the layer it
// is at. It is a value: an operation returns another flow, the one given
// unchanged.
type Flow[S, T any] struct {
	from   []actor.Dest // the sources of the first layer
	done   []layer      // the layers before, a Partition ending each
	stages int          // the workers of this layer, Run's Workers if 0
	// xf is the operations fused: given what takes an event of T, it
	// returns what takes one of S, giving it what the operations make of
	// it.
	xf func(emit func(T)) func(S)
}

// From is the flow of the events of the stages at from, producers or
// producer-consumers of events of type S, of this node or another.
func From[S any](from ...actor.Dest) Flow[S, S] {
	return Flow[S, S]{from: from, xf: identity[S]}
}

func identity[T any](emit func(T)) func(T) { return emit }

// Map is f, each event of it made fn of it.
func Map[S, T, U any](f Flow[S, T], fn func(T) U) Flow[S, U] {
	return Flow[S, U]{from: f.from, done: f.done, stages: f.stages, xf: func(emit func(U)) func(S) {
		return f.xf(func(t T) { emit(fn(t)) })
	}}
}

// Filter is f, but the events keep reports false of.
func Filter[S, T any](f Flow[S, T], keep func(T) bool) Flow[S, T] {
	return Flow[S, T]{from: f.from, done: f.done, stages: f.stages, xf: func(emit func(T)) func(S) {
		return f.xf(func(t T) {
			if keep(t) {
				emit(t)
			}
		})
	}}
}

// FlatMap is f, each event of it made the events fn gives of it, none or
// several, through yield.
func FlatMap[S, T, U any](f Flow[S, T], fn func(t T, yield func(U))) Flow[S, U] {
	return Flow[S, U]{from: f.from, done: f.done, stages: f.stages, xf: func(emit func(U)) func(S) {
		return f.xf(func(t T) { fn(t, emit) })
	}}
}

// Partition ends the layer of f, and starts a layer of n workers, each
// event of f going to the one of its key: the events of a key all to one
// worker, as GenStage's PartitionDispatcher sends them, for the next
// operations, a Reduce, to have them together. A worker slow, the others
// wait for it, rather than any event be lost.
func Partition[S, T any, K comparable](f Flow[S, T], n int, key func(T) K) Flow[T, T] {
	seed := maphash.MakeSeed()
	hash := func(t T) int { return int(maphash.Comparable(seed, key(t)) % uint64(max(n, 1))) }
	l := layerOf(f, stage.Partition[T]{Partitions: n, Hash: hash})
	return Flow[T, T]{from: f.from, done: append(slices.Clip(f.done), l), stages: n, xf: identity[T]}
}
