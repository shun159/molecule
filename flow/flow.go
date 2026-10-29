package flow

import (
	"github.com/shun159/molecule/actor"
)

// Flow is a plan of work: the events of its sources, of type S, made
// events of type T by the operations fused on them. It is a value: an
// operation returns another flow, the one given unchanged.
type Flow[S, T any] struct {
	from []actor.Dest
	// xf is the operations fused: given what takes an event of T, it
	// returns what takes one of S, giving it what the operations make of
	// it.
	xf func(emit func(T)) func(S)
}

// From is the flow of the events of the stages at from, producers or
// producer-consumers of events of type S, of this node or another.
func From[S any](from ...actor.Dest) Flow[S, S] {
	return Flow[S, S]{from: from, xf: func(emit func(S)) func(S) { return emit }}
}

// Map is f, each event of it made fn of it.
func Map[S, T, U any](f Flow[S, T], fn func(T) U) Flow[S, U] {
	return Flow[S, U]{from: f.from, xf: func(emit func(U)) func(S) {
		return f.xf(func(t T) { emit(fn(t)) })
	}}
}

// Filter is f, but the events keep reports false of.
func Filter[S, T any](f Flow[S, T], keep func(T) bool) Flow[S, T] {
	return Flow[S, T]{from: f.from, xf: func(emit func(T)) func(S) {
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
	return Flow[S, U]{from: f.from, xf: func(emit func(U)) func(S) {
		return f.xf(func(t T) { fn(t, emit) })
	}}
}
