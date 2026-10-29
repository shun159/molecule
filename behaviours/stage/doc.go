// Package stage is the behaviour of stages exchanging events with back
// pressure, like Elixir's GenStage: a consumer asks its producer for
// events, and the producer sends no more than asked. Reading a file or a
// socket no faster than it is handled, a pipeline holds a bounded number
// of events in flight, in no mailbox more than its consumer asked for.
//
// A stage is a [Producer], a [ProducerConsumer] or a [Consumer], made
// one to start by [Produce], [ProduceConsume] or [Consume], and started
// with [Start], [StartLink] or as a [Child]:
//
//	src, _ := stage.Start(ctx, n, stage.Produce(Lines{R: r}))
//	words, _ := stage.Start(ctx, n, stage.ProduceConsume(Split{From: src}))
//	stage.Start(ctx, n, stage.Consume(Count{From: words}))
//
// # Callbacks
//
//	Producer
//	Init(self) (S, []actor.Effect, error)                init/1
//	HandleDemand(S, n) (S, []actor.Effect)               handle_demand/2
//
//	Consumer, and ProducerConsumer, which emits
//	Init(self) (S, []actor.Effect, error)                init/1
//	HandleEvents(S, []In, tag) (S, []actor.Effect)       handle_events/3
//
//	Consumer and ProducerConsumer
//	HandleCancel(S, tag, error) (S, []actor.Effect)      handle_cancel/3, optional
//
//	All
//	HandleInfo(S, any) (S, []actor.Effect)               handle_info/2, optional
//	Terminate(S, error) []actor.Effect                   terminate/2, optional
//
// The callbacks are pure, as those of genserver, and return the effects
// of actor with those of stage: [Emit] and [Done] for a producer,
// [Subscribe] and [Cancel] for a consumer, all for a producer-consumer.
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
// producer, and are sent first as demand comes: as many as its [Buffer]
// holds, DefaultBuffer unless it implements [Buffering], those beyond
// dropped, or stopping it.
//
// A producer-consumer handles events only as its consumers ask for them,
// and asks its producers for more only once it has handled those: the
// demand goes up a pipeline from its end.
//
// # Dispatchers
//
// A producer, or a producer-consumer, has any number of consumers, and
// sends them its events as its [Dispatcher] says, which it tells by
// implementing [Dispatching]:
//
//	Demand     each event to one consumer, the one wanting most: the
//	           consumers share the work, each as fast as it goes; the
//	           default
//	Broadcast  each event to every consumer, as fast as the slowest
//	Partition  each event to the consumer of its partition, by a hash of
//	           it: a consumer takes one with Subscribe
//
// HandleDemand is told of the events the consumers want, as the
// dispatcher counts them: for Broadcast, those the consumer wanting least
// wants, for Partition, those of all but as many as wait in partitions
// wanting none.
//
// # Nodes
//
// Stages of different nodes subscribe to each other as to those of their
// own, by PID or by actor.Remote name, once dist connects the nodes. Each
// node registers the types of the events it sends or receives with
// [Register], at init, and they travel by gob, with their exported
// fields; the messages of the protocol encode themselves. The demand
// bounds what is in flight between the nodes as within one: a consumer
// asks for more as it handles what it has, and a large Max keeps the
// connection busy, the batches large, as each costs a round trip.
//
// A node lost, its subscriptions end with proc.NoConnection, as monitors
// do: the consumers stop, or go on, as their Ending says, and the
// producers forget the consumers gone. Reasons cross nodes as dist sends
// exit reasons, those of stage kept, for errors.Is to tell them.
//
// # Ending
//
// Each end of a subscription monitors the other. A consumer stops when a
// subscription of it ends, with the reason its producer exited with, or
// cancelled it with: a producer done stops, normally, and so does its
// consumer, once it has handled all the producer sent. The [Ending] of
// the subscription says otherwise: Transient goes on after a normal end,
// Temporary after any, as the consumer of several producers, one done,
// may. A producer having
// emitted all returns Done, to stop once it has sent what it holds: a
// Stop would drop it. A consumer subscribing to a producer once it is
// gone stops with proc.NoProc; a producer [Accumulating] holds the demand
// until it returns [Forward], for all its consumers to subscribe first. A producer-consumer
// ending normally stops once it has handled what it holds of the producer
// and sent its consumers what it emitted, so that a pipeline ends once all
// is through. A consumer cancelling, or exiting, leaves its producer to
// the others: the events emitted for it go to them.
package stage
