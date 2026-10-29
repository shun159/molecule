package stage

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/dist"
)

// Stats tells how a stage is doing, for an operator to see where a
// pipeline is held up: a buffer growing has consumers too slow, demand
// with nothing emitted a source too slow, subscriptions waiting a node
// gone. The counts are since the stage started.
type Stats struct {
	// The side emitting, of a producer or a producer-consumer.
	Consumers int    // subscribed to it
	Demand    int    // events it may emit now, its consumers wanting them
	Held      int    // events emitted beyond the demand, waiting for it
	Emitted   uint64 // events it emitted
	Dropped   uint64 // events it dropped, its Buffer full

	// The side subscribing, of a consumer or a producer-consumer.
	Subscriptions int    // to producers
	Waiting       int    // subscriptions Resubscribe waiting to subscribe again
	Pending       int    // events asked for and not received
	Queued        int    // events received and not handled yet
	Received      uint64 // events received
}

// statsReq is the call StatsOf makes.
type statsReq struct{}

func init() { dist.Register(statsReq{}, Stats{}) }

// StatsOf returns the Stats of the stage at to, of this node or another.
func StatsOf(ctx context.Context, caller actor.Caller, to actor.Dest) (Stats, error) {
	v, err := actor.Call(ctx, caller, to, statsReq{})
	if err != nil {
		return Stats{}, err
	}
	return v.(Stats), nil
}

// isStatsCall reports whether msg is the call of StatsOf, and returns its
// From.
func isStatsCall(msg any) (actor.From, bool) {
	c, ok := msg.(actor.CallMsg)
	if !ok {
		return actor.From{}, false
	}
	_, ok = c.Req.(statsReq)
	return c.From, ok
}

// stats is the Stats of the sides of a stage, nil if it has not, and of
// the events it queued.
func stats(out *emitting, in *subscribing, queued int) Stats {
	var st Stats
	if out != nil {
		st.Consumers, st.Demand, st.Held = out.to.consumerCount(), out.to.capacity(), out.to.held()
		st.Emitted, st.Dropped = out.emitted, out.dropped
	}
	if in != nil {
		st.Subscriptions, st.Waiting, st.Received = len(in.subs), len(in.waiting), in.received
		for _, s := range in.subs {
			st.Pending += s.pending
		}
	}
	st.Queued = queued
	return st
}
