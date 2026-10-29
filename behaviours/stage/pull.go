package stage

import (
	"context"
	"iter"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Pull subscribes to the stage at sub.To, from outside the processes, and
// returns its events to range over, as a Go iterator would: each event,
// and, if the subscription ends other than normally, its reason once, at
// the end. It asks for Max events ahead, DefaultMax if zero, and for as
// many more as the range takes: no more than Max wait. Breaking off the
// range cancels the subscription; an Ephemeral producer then stops.
//
//	for v, err := range stage.Pull[Lease](ctx, node, stage.Subscribe{To: ranger}) {
//
// Behind it, a process of n subscribes, and keeps the events received
// until the range takes them. Of sub, Tag, Min and Ending are not used.
func Pull[T any](ctx context.Context, n *proc.Node, sub Subscribe) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		pid, err := gen.Start(ctx, n, puller[T]{sub: sub}, nil)
		if err != nil {
			yield(zero, err)
			return
		}
		defer actor.SendCast(n, pid, stopPulling{})
		for {
			v, err := actor.Call(ctx, n, pid, takeReq{})
			if err != nil {
				yield(zero, err)
				return
			}
			r := v.(taken[T])
			for _, e := range r.events {
				if !yield(e, nil) {
					return
				}
			}
			if r.end {
				if !normal(r.reason) {
					yield(zero, r.reason)
				}
				return
			}
		}
	}
}

type (
	// takeReq takes the events received, waiting for some if none.
	takeReq struct{}
	// taken is what a takeReq took: events, or the end of the
	// subscription, with its reason.
	taken[T any] struct {
		events []T
		end    bool
		reason error
	}
	// stopPulling stops the puller.
	stopPulling struct{}
)

// puller is the process of Pull: a consumer taking its events as they are
// taken from it, rather than as it handles them.
type puller[T any] struct {
	sub Subscribe
}

// pulling is the state of a puller.
type pulling[T any] struct {
	in      subscribing
	ref     subRef
	queue   []T        // the events received, not taken
	lent    int        // the events taken last, the range going over them
	taker   actor.From // a take waiting, if waiting
	waiting bool
	ended   bool
	reason  error
}

func (p puller[T]) Init(self proc.PID, _ any) (pulling[T], []actor.Effect, error) {
	s := pulling[T]{in: subscribing{self: self}}
	sub := p.sub
	sub.Tag, sub.Min, sub.Ending = nil, 0, Permanent
	out := s.in.subscribe(sub)
	if stops(out) {
		return s, nil, ErrBadDemand
	}
	s.ref = s.in.subs[0].ref
	return s, out, nil
}

func (p puller[T]) Handle(s pulling[T], msg gen.Msg) (pulling[T], []actor.Effect) {
	switch m := msg.(type) {
	case gen.InfoMsg:
		return p.info(s, m.Msg)
	case actor.CallMsg:
		if _, ok := m.Req.(takeReq); ok {
			return s.take(m.From)
		}
	case actor.CastMsg:
		if _, ok := m.Req.(stopPulling); ok {
			return s, actor.Do(actor.Stop{})
		}
	}
	return s, nil
}

// info keeps the events of the subscription, and its end.
func (p puller[T]) info(s pulling[T], msg any) (pulling[T], []actor.Effect) {
	switch m := msg.(type) {
	case eventsMsg:
		events, ok := eventsOf[T](m)
		if !ok {
			return s.end(&BadEventsError{Events: m.Events})
		}
		s.queue = append(slices.Clip(s.queue), events...)
		return s.give()
	case cancelledMsg:
		return s.end(m.Reason)
	case actor.Down:
		if _, ok := m.Tag.(producerDown); ok {
			return s.end(m.Reason)
		}
	}
	return s, nil
}

// end ends the subscription, with reason.
func (s pulling[T]) end(reason error) (pulling[T], []actor.Effect) {
	if reason == nil {
		reason = proc.Normal
	}
	s.ended, s.reason = true, reason
	return s.give()
}

// take takes the events received for from, the range done with those it
// took last: they are asked for again, so that the range, the queue and
// what is on its way are never more than Max.
func (s pulling[T]) take(from actor.From) (pulling[T], []actor.Effect) {
	var out []actor.Effect
	if s.lent > 0 && !s.ended {
		out = actor.Do(actor.Send{To: s.in.subs[0].to, Msg: askMsg{Ref: s.ref, N: s.lent}})
	}
	s.taker, s.waiting, s.lent = from, true, 0
	s, more := s.give()
	return s, append(out, more...)
}

// give answers the take waiting, if any, with the events received; or
// with the end, once all are taken.
func (s pulling[T]) give() (pulling[T], []actor.Effect) {
	switch {
	case !s.waiting:
		return s, nil
	case len(s.queue) > 0:
		events := s.queue
		s.queue, s.waiting, s.lent = nil, false, len(events)
		return s, actor.Do(actor.Reply{To: s.taker, Value: taken[T]{events: events}})
	case s.ended:
		s.waiting = false
		return s, actor.Do(actor.Reply{To: s.taker, Value: taken[T]{end: true, reason: s.reason}})
	}
	return s, nil
}

func (puller[T]) Terminate(pulling[T], error) []actor.Effect { return nil }

// Label tells what the process is.
func (puller[T]) Label() string { return "stage pull" }
