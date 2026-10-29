package stage

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// subscribing is the side of a stage that subscribes, a consumer or a
// producer-consumer: its subscriptions to producers.
type subscribing struct {
	self     proc.PID
	subs     []subscription // copied when changed
	waiting  []subscription // ended, to subscribe again, see retry
	next     uint64         // the N of the next subRef
	received uint64         // events, since the start, see Stats
}

// subscription is a subscription to a producer.
type subscription struct {
	ref       subRef
	to        actor.Dest
	tag       any
	max, min  int
	partition int
	pending   int // the events asked for and not received
	ending    Ending
	wait      time.Duration // before subscribing again, if Resubscribe; 0 once events came
	held      bool          // asking for none until released
}

// take counts n events received, and returns how many to ask for: once
// the pending demand falls to min, as many as bring it back to max. Events
// come, a subscription Resubscribe waits the least again, see retry.
func (s *subscription) take(n int) int {
	s.pending = max(s.pending-n, 0)
	s.wait = 0
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
	sub := subscription{ref: ref, to: x.To, tag: x.Tag, max: maxDemand, min: minDemand,
		partition: x.Partition, pending: maxDemand, ending: x.Ending}
	if x.Held {
		sub.pending, sub.held = 0, true
	}
	in.subs = append(slices.Clip(in.subs), sub)
	return append(out,
		actor.Monitor{Target: x.To, Tag: producerDown{ref}},
		actor.Send{To: x.To, Msg: subscribeMsg{Ref: ref, Ask: sub.pending, Partition: x.Partition}},
	)
}

// ask is the effect asking the producer of s for n more events.
func (s *subscription) ask(n int) actor.Effect {
	return actor.Send{To: s.to, Msg: askMsg{Ref: s.ref, N: n}}
}

// release has the subscription under x.Tag, held, ask for Max events.
func (in *subscribing) release(x Release) []actor.Effect {
	i := slices.IndexFunc(in.subs, func(s subscription) bool { return s.tag == x.Tag })
	if i < 0 || !in.subs[i].held {
		return nil
	}
	in.subs = slices.Clone(in.subs)
	sub := &in.subs[i]
	sub.held, sub.pending = false, sub.max
	return actor.Do(sub.ask(sub.max))
}

// cancel cancels the subscription under x.Tag, if any, on or waiting to
// subscribe again.
func (in *subscribing) cancel(x Cancel) []actor.Effect {
	i := slices.IndexFunc(in.subs, func(s subscription) bool { return s.tag == x.Tag })
	if i < 0 {
		return in.stopWaiting(x.Tag)
	}
	sub := in.subs[i]
	in.subs = slices.Delete(slices.Clone(in.subs), i, i+1)
	return actor.Do(
		actor.Demonitor{Tag: producerDown{sub.ref}},
		actor.Send{To: sub.to, Msg: cancelMsg{Ref: sub.ref, Reason: x.Reason}},
	)
}

// end forgets the subscription of ref its producer ended, and returns
// it, if there was one.
func (in *subscribing) end(ref subRef) (subscription, bool) {
	i := in.find(ref)
	if i < 0 {
		return subscription{}, false
	}
	sub := in.subs[i]
	in.subs = slices.Delete(slices.Clone(in.subs), i, i+1)
	return sub, true
}

// receiving readies the subscriptions for n events received: their
// pending demands change, in a copy.
func (in *subscribing) receiving(n int) {
	in.subs = slices.Clone(in.subs)
	in.received += uint64(n)
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
