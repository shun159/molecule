// Package stage is the behaviour of stages exchanging events with back
// pressure, like Elixir's GenStage: a consumer asks its producer for
// events, and the producer sends no more than asked. Reading a file or a
// socket no faster than it is handled, a pipeline holds a bounded number
// of events in flight, in no mailbox more than its consumer asked for.
//
// A stage is a [Producer] or a [Consumer], made one to start by [Produce]
// or [Consume], and started with [Start], [StartLink] or as a [Child]:
//
//	src, _ := stage.Start(ctx, n, stage.Produce(Lines{R: r}))
//	stage.Start(ctx, n, stage.Consume(Count{From: src}))
//
// # Callbacks
//
//	Producer
//	Init(self) (S, []actor.Effect, error)                init/1
//	HandleDemand(S, n) (S, []actor.Effect)               handle_demand/2
//
//	Consumer
//	Init(self) (S, []actor.Effect, error)                init/1
//	HandleEvents(S, []In, tag) (S, []actor.Effect)       handle_events/3
//
//	Both
//	HandleInfo(S, any) (S, []actor.Effect)               handle_info/2, optional
//	Terminate(S, error) []actor.Effect                   terminate/2, optional
//
// The callbacks are pure, as those of genserver, and return the effects
// of actor with those of stage: [Emit] for a producer, [Subscribe] and
// [Cancel] for a consumer.
//
// # Demand
//
// A consumer subscribes with Subscribe, in Init or later, asking for Max
// events. It handles them Max-Min at most at a time, and once those asked
// for and not received fall to Min, asks for as many as bring them back
// to Max. The producer is told of each ask by HandleDemand, with how many
// more events are wanted, and emits them with Emit: at once, or later, as
// they come, from an actor.Async reading a file, say, whose outcome
// arrives in HandleInfo. Events emitted beyond the demand wait in the
// producer, and are sent first as demand comes.
//
// A producer has one consumer at most: another subscribing is cancelled
// with [ErrHasConsumer].
//
// # Ending
//
// Each end of a subscription monitors the other. A consumer stops when a
// subscription of it ends, with the reason its producer exited with, or
// cancelled it with: a producer done stops, normally, and so does its
// consumer, once it has handled all the producer sent. A consumer
// cancelling, or exiting, frees its producer for another.
package stage
