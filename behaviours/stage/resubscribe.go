package stage

import (
	"slices"
	"time"

	"github.com/shun159/molecule/actor"
)

// A subscription Resubscribe its producer ends waits, then subscribes
// again: after resubscribeMin, then twice as long each time it fails
// again, up to resubscribeMax; the first events received have it wait
// resubscribeMin again.
const (
	resubscribeMin = 100 * time.Millisecond
	resubscribeMax = 5 * time.Second
)

type (
	// resubscribeKey is the key of the timer of the subscription under
	// tag waiting to subscribe again.
	resubscribeKey struct{ tag any }
	// resubscribeMsg is the message of that timer.
	resubscribeMsg struct{ tag any }
)

// retry has the subscription sub, ended, subscribe again after a while,
// if Resubscribe.
func (in *subscribing) retry(sub subscription) []actor.Effect {
	if sub.ending != Resubscribe {
		return nil
	}
	sub.wait = min(max(2*sub.wait, resubscribeMin), resubscribeMax)
	in.waiting = append(slices.Clip(in.waiting), sub)
	return actor.Do(actor.StartTimer{Key: resubscribeKey{sub.tag}, After: sub.wait, Msg: resubscribeMsg{sub.tag}})
}

// resubscribe subscribes again the subscription m tells of, waiting, if
// it is still: neither cancelled nor subscribed anew meanwhile.
func (in *subscribing) resubscribe(m resubscribeMsg) []actor.Effect {
	i := in.findWaiting(m.tag)
	if i < 0 {
		return nil
	}
	sub := in.waiting[i]
	in.waiting = slices.Delete(slices.Clone(in.waiting), i, i+1)
	x := Subscribe{To: sub.to, Tag: sub.tag, Max: sub.max, Min: sub.min, Partition: sub.partition, Ending: Resubscribe}
	out := in.subscribe(x)
	in.subs[len(in.subs)-1].wait = sub.wait
	return out
}

// stopWaiting forgets the subscription under tag waiting to subscribe
// again, if any, and cancels its timer.
func (in *subscribing) stopWaiting(tag any) []actor.Effect {
	i := in.findWaiting(tag)
	if i < 0 {
		return nil
	}
	in.waiting = slices.Delete(slices.Clone(in.waiting), i, i+1)
	return actor.Do(actor.CancelTimer{Key: resubscribeKey{tag}})
}

func (in *subscribing) findWaiting(tag any) int {
	return slices.IndexFunc(in.waiting, func(s subscription) bool { return s.tag == tag })
}
