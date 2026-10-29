package stage

import "slices"

// batch is events of one type, emitted, for a producer to send and
// buffer without knowing their type: a producer is typed by its state
// alone, for Produce to infer it.
type batch interface {
	len() int
	// tail is the events after the first n, nil if none.
	tail(n int) batch
	// first is the first n events, nil if none.
	first(n int) batch
	// join is the batch with more after it, of the same type.
	join(more batch) batch
	// sameType reports whether b holds events of the same type.
	sameType(b batch) bool
	// empty is no events, of the same type.
	empty() batch
	// partition splits the events by partition, from 0 to n-1, as hash, a
	// func(T) int of their type T, tells, dropping those of a negative one.
	partition(hash any, n int) ([]batch, error)
}

// events are the events of an Emit[T].
type events[T any] []T

func (e events[T]) len() int { return len(e) }

func (e events[T]) tail(n int) batch {
	if n == len(e) {
		return nil
	}
	return e[n:]
}

func (e events[T]) first(n int) batch {
	if n == 0 {
		return nil
	}
	return e[:n:n]
}

func (e events[T]) join(more batch) batch {
	return append(slices.Clip(e), more.(events[T])...)
}

func (e events[T]) sameType(b batch) bool {
	_, ok := b.(events[T])
	return ok
}

func (events[T]) empty() batch { return events[T](nil) }

func (e events[T]) partition(hash any, n int) ([]batch, error) {
	h, ok := hash.(func(T) int)
	if !ok {
		return nil, errEmitType
	}
	split := make([]events[T], n)
	for _, ev := range e {
		k := h(ev)
		if k >= n {
			return nil, ErrBadPartition
		}
		if k >= 0 {
			split[k] = append(split[k], ev)
		}
	}
	parts := make([]batch, n)
	for k, p := range split {
		if len(p) > 0 {
			parts[k] = p
		}
	}
	return parts, nil
}

// eventsOf returns the events of m, if of type In.
func eventsOf[In any](m eventsMsg) ([]In, bool) {
	e, ok := m.Events.(events[In])
	return e, ok
}

// emitter is an Emit of any type.
type emitter interface {
	batch() batch
}

func (x Emit[T]) batch() batch { return events[T](x.Events) }
