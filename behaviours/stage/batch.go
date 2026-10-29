package stage

import "slices"

// batch is events of one type, emitted, for a producer to send and
// buffer without knowing their type: a producer is typed by its state
// alone, for Produce to infer it.
type batch interface {
	len() int
	// head is the first n events, a slice of their type, to send.
	head(n int) any
	// tail is the events after the first n, nil if none.
	tail(n int) batch
	// join is the batch with more after it, of the same type.
	join(more batch) batch
	// sameType reports whether b holds events of the same type.
	sameType(b batch) bool
	// empty is no events, of the same type.
	empty() batch
}

// events are the events of an Emit[T].
type events[T any] []T

func (e events[T]) len() int { return len(e) }

func (e events[T]) head(n int) any { return []T(e[:n:n]) }

func (e events[T]) tail(n int) batch {
	if n == len(e) {
		return nil
	}
	return e[n:]
}

func (e events[T]) join(more batch) batch {
	return append(slices.Clip(e), more.(events[T])...)
}

func (e events[T]) sameType(b batch) bool {
	_, ok := b.(events[T])
	return ok
}

func (events[T]) empty() batch { return events[T](nil) }

// emitter is an Emit of any type.
type emitter interface {
	batch() batch
}

func (x Emit[T]) batch() batch { return events[T](x.Events) }
