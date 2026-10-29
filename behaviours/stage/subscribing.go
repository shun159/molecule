package stage

import (
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// subscribing is the side of a stage that subscribes, a consumer or a
// producer-consumer: its subscriptions to producers.
type subscribing struct {
	self proc.PID
	subs []subscription // copied when changed
	next uint64         // the N of the next subRef
}

// subscription is a subscription to a producer.
type subscription struct {
	ref      subRef
	to       actor.Dest
	tag      any
	max, min int
	pending  int // the events asked for and not received
}

// take counts n events received, and returns how many to ask for: once
// the pending demand falls to min, as many as bring it back to max.
func (s *subscription) take(n int) int {
	s.pending = max(s.pending-n, 0)
	if s.pending > s.min {
		return 0
	}
	ask := s.max - s.pending
	s.pending = s.max
	return ask
}

// batchSize is how many events are handled at most at a time.
func (s *subscription) batchSize() int { return s.max - s.min }

// subscribe subscribes to the producer at x.To, which it monitors, asking
// for Max events.
func (in *subscribing) subscribe(x Subscribe) []actor.Effect {
	maxDemand, minDemand, ok := x.demand()
	if !ok {
		return actor.Do(actor.Stop{Reason: ErrBadDemand})
	}
	out := in.cancel(Cancel{Tag: x.Tag})
	ref := subRef{Consumer: in.self, N: in.next}
	in.next++
	sub := subscription{ref: ref, to: x.To, tag: x.Tag, max: maxDemand, min: minDemand, pending: maxDemand}
	in.subs = append(slices.Clip(in.subs), sub)
	return append(out,
		actor.Monitor{Target: x.To, Tag: producerDown{ref}},
		actor.Send{To: x.To, Msg: subscribeMsg{Ref: ref, Ask: maxDemand}},
	)
}

// cancel cancels the subscription under x.Tag, if any.
func (in *subscribing) cancel(x Cancel) []actor.Effect {
	i := slices.IndexFunc(in.subs, func(s subscription) bool { return s.tag == x.Tag })
	if i < 0 {
		return nil
	}
	sub := in.subs[i]
	in.subs = slices.Delete(slices.Clone(in.subs), i, i+1)
	return actor.Do(
		actor.Demonitor{Tag: producerDown{sub.ref}},
		actor.Send{To: sub.to, Msg: cancelMsg{Ref: sub.ref, Reason: x.Reason}},
	)
}

// end forgets the subscription of ref its producer ended, and reports
// whether there was one.
func (in *subscribing) end(ref subRef) bool {
	i := in.find(ref)
	if i < 0 {
		return false
	}
	in.subs = slices.Delete(slices.Clone(in.subs), i, i+1)
	return true
}

// askMore appends to out the ask of the producer of ref for n more
// events, if n > 0 and the subscription is still on.
func (in *subscribing) askMore(ref subRef, n int, out []actor.Effect) []actor.Effect {
	i := in.find(ref)
	if i < 0 || n == 0 {
		return out
	}
	return append(out, actor.Send{To: in.subs[i].to, Msg: askMsg{Ref: ref, N: n}})
}

// find returns the index of the subscription of ref, or -1.
func (in *subscribing) find(ref subRef) int {
	return slices.IndexFunc(in.subs, func(s subscription) bool { return s.ref == ref })
}
