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
	return PullAll[T](ctx, n, sub)
}

// PullAll is Pull of several stages at once: their events as they come,
// to the end of all, or of the first ending other than normally, its
// reason then the last. Each has Max of its own.
func PullAll[T any](ctx context.Context, n *proc.Node, subs ...Subscribe) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		p, err := StartPull[T](ctx, n, subs...)
		if err != nil {
			var zero T
			yield(zero, err)
			return
		}
		for v, err := range p.All() {
			if !yield(v, err) {
				return
			}
		}
	}
}

// Puller is a PullAll started: its subscriptions made, its events to
// range over with All, once only.
type Puller[T any] struct {
	ctx context.Context
	n   *proc.Node
	pid proc.PID
}

// StartPull starts PullAll, and returns once it has subscribed: for what
// is to be done after, such as having the stages it subscribed to start,
// before any waits for them; then to range over its All.
func StartPull[T any](ctx context.Context, n *proc.Node, subs ...Subscribe) (*Puller[T], error) {
	pid, err := gen.Start(ctx, n, puller[T]{subs: subs}, nil)
	if err != nil {
		return nil, err
	}
	return &Puller[T]{ctx: ctx, n: n, pid: pid}, nil
}

// All returns the events, as PullAll does. Its range done, or broken off,
// the subscriptions are cancelled.
func (p *Puller[T]) All() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		defer p.Stop()
		for {
			v, err := actor.Call(p.ctx, p.n, p.pid, takeReq{})
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

// Stop cancels the subscriptions, for a Puller never ranged over.
func (p *Puller[T]) Stop() { actor.SendCast(p.n, p.pid, stopPulling{}) }

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
	subs []Subscribe
}

// pulling is the state of a puller.
type pulling[T any] struct {
	in      subscribing
	queue   []T        // the events received, not taken
	from    []owed     // how many of the queue each subscription sent
	lent    []owed     // the same, of the events taken last, the range going over them
	taker   actor.From // a take waiting, if waiting
	waiting bool
	ended   bool // all the subscriptions, or one other than normally
	reason  error
}

// owed is events a subscription sent, to ask it for again once the range
// is done with them.
type owed struct {
	ref subRef
	n   int
}

func (p puller[T]) Init(self proc.PID, _ any) (pulling[T], []actor.Effect, error) {
	s := pulling[T]{in: subscribing{self: self}}
	var out []actor.Effect
	for i, sub := range p.subs {
		sub.Tag, sub.Min, sub.Ending = i, 0, Permanent
		if out = append(out, s.in.subscribe(sub)...); stops(out) {
			return s, nil, ErrBadDemand
		}
	}
	if len(p.subs) == 0 {
		s.ended, s.reason = true, proc.Normal
	}
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

// info keeps the events of the subscriptions, and their ends.
func (p puller[T]) info(s pulling[T], msg any) (pulling[T], []actor.Effect) {
	switch m := msg.(type) {
	case eventsMsg:
		return s.received(m)
	case cancelledMsg:
		return s.endOf(m.Ref, m.Reason)
	case actor.Down:
		if t, ok := m.Tag.(producerDown); ok {
			return s.endOf(t.ref, m.Reason)
		}
	}
	return s, nil
}

// received keeps the events of m, if of a subscription on.
func (s pulling[T]) received(m eventsMsg) (pulling[T], []actor.Effect) {
	if s.in.find(m.Ref) < 0 {
		return s, nil
	}
	events, ok := eventsOf[T](m)
	if !ok {
		return s.end(&BadEventsError{Events: m.Events})
	}
	s.queue = append(slices.Clip(s.queue), events...)
	s.from = addOwed(s.from, m.Ref, len(events))
	return s.give()
}

// endOf ends the subscription of ref: the range ends with it if not
// normally, or once all have.
func (s pulling[T]) endOf(ref subRef, reason error) (pulling[T], []actor.Effect) {
	if _, ok := s.in.end(ref); !ok {
		return s, nil
	}
	if !normal(reason) || len(s.in.subs) == 0 {
		return s.end(reason)
	}
	return s, nil
}

// end ends the range, with reason.
func (s pulling[T]) end(reason error) (pulling[T], []actor.Effect) {
	if reason == nil {
		reason = proc.Normal
	}
	s.ended, s.reason = true, reason
	return s.give()
}

// take takes the events received for from, the range done with those it
// took last: they are asked for again, so that the range, the queue and
// what is on its way are never more than Max, of each subscription.
func (s pulling[T]) take(from actor.From) (pulling[T], []actor.Effect) {
	var out []actor.Effect
	for _, o := range s.lent {
		out = s.in.askMore(o.ref, o.n, out)
	}
	s.taker, s.waiting, s.lent = from, true, nil
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
		s.queue, s.waiting = nil, false
		s.lent, s.from = s.from, nil
		return s, actor.Do(actor.Reply{To: s.taker, Value: taken[T]{events: events}})
	case s.ended:
		s.waiting = false
		return s, actor.Do(actor.Reply{To: s.taker, Value: taken[T]{end: true, reason: s.reason}})
	}
	return s, nil
}

// addOwed returns os with n more owed to ref.
func addOwed(os []owed, ref subRef, n int) []owed {
	i := slices.IndexFunc(os, func(o owed) bool { return o.ref == ref })
	if i < 0 {
		return append(slices.Clip(os), owed{ref: ref, n: n})
	}
	os = slices.Clone(os)
	os[i].n += n
	return os
}

func (puller[T]) Terminate(pulling[T], error) []actor.Effect { return nil }

// Label tells what the process is.
func (puller[T]) Label() string { return "stage pull" }
