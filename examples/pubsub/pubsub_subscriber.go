package main

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist/pg"
	"github.com/shun159/molecule/proc"
)

// zeroPID is no process: pg.SendEffect excepts none.
var zeroPID proc.PID

// subsName is the supervisor of the subscribers of a node.
var subsName = actor.Local("pubsub_subs")

// Subscriber is subscribed to Topic, and passes what is published on it
// to Out. It joins the topic as it starts, and leaves it as it ends, as
// pg takes a process that exits out of its groups.
type Subscriber struct {
	genserver.Default[struct{}]
	Topic string
	Out   proc.PID
}

func (s Subscriber) Init(self proc.PID) (struct{}, []actor.Effect, error) {
	return struct{}{}, actor.Do(pg.JoinEffect(scope, s.Topic, self)), nil
}

func (s Subscriber) HandleInfo(st struct{}, msg any) (struct{}, []actor.Effect) {
	if p, ok := msg.(Published); ok {
		return st, actor.Do(actor.Send{To: s.Out, Msg: p})
	}
	return st, nil
}

// Subscribe starts a subscriber to topic on the node of caller, passing
// what is published on it to out, and returns it.
func Subscribe(ctx context.Context, caller actor.Caller, topic string, out proc.PID) (proc.PID, error) {
	spec := supervisor.ChildSpec{Start: genserver.Child(Subscriber{Topic: topic, Out: out}), Restart: supervisor.Transient}
	return supervisor.StartChild(ctx, caller, subsName, spec)
}

// Unsubscribe stops the subscriber sub.
func Unsubscribe(ctx context.Context, caller actor.Caller, sub proc.PID) error {
	return supervisor.TerminateChild(ctx, caller, subsName, sub)
}
