package stage

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Stage is a producer or a consumer, as Produce and Consume make it, to
// start, or to simulate with gensim, a gen.Behaviour.
type Stage[S any] interface {
	gen.Behaviour[S]
	stage()
}

// Start starts st in a new process. It may be called from outside any
// process.
func Start[S any](ctx context.Context, n *proc.Node, st Stage[S], opts ...actor.Option) (proc.PID, error) {
	return gen.Start(ctx, n, st, nil, opts...)
}

// StartLink starts st in a new process linked to parent.
func StartLink[S any](ctx context.Context, parent *proc.Self, st Stage[S], opts ...actor.Option) (proc.PID, error) {
	return gen.StartLink(ctx, parent, st, nil, opts...)
}

// Child returns st as a child to start, the Start of a
// supervisor.ChildSpec, which gensim can simulate as well.
func Child[S any](st Stage[S], opts ...actor.Option) gen.Child {
	return gen.ChildOf(st, nil, opts...)
}

// InfoHandler handles the messages that are not of the stages: plain
// messages, the outcomes of effects, calls and casts as actor.CallMsg and
// actor.CastMsg, and the Msg of a Continue. Without it, they are dropped.
type InfoHandler[S any] interface {
	HandleInfo(state S, msg any) (S, []actor.Effect)
}

// Terminator is called when the stage stops, see gen.Behaviour. The
// effects of stage it returns are ignored.
type Terminator[S any] interface {
	Terminate(state S, reason error) []actor.Effect
}

// Emit sends Events to the consumers of a producer, as far as they demand
// them; the rest wait in the producer, sent first as demand comes. The
// Events are not to be written once emitted.
type Emit[T any] struct {
	actor.Extension
	Events []T
}

// Done tells that a producer, or a producer-consumer, has emitted all
// its events: it stops, normally, once it has sent its consumers those it
// holds; a producer-consumer, once it has handled the events it holds of
// its producers as well, and sent what it emitted of them. A Stop does
// not wait, and those are lost.
type Done struct {
	actor.Extension
}

// Forward ends the holding of the demand of a producer, or a
// producer-consumer, Accumulating: it is told of the demand held, and of
// all after, as GenStage.demand(stage, :forward).
type Forward struct {
	actor.Extension
}

// Accumulating is a producer, or a producer-consumer, holding the demand
// of its consumers from its start until it returns Forward, as GenStage's
// demand: :accumulate: so that all its consumers subscribe before it
// emits, none coming late to it done. A producer-consumer handles no
// events meanwhile.
type Accumulating interface {
	Accumulate() bool
}

// Ephemeral is a producer, or a producer-consumer, serving its consumers
// alone: once it has had one, it stops, normally, as the last is gone,
// cancelling or exiting, as GenStage.from_enumerable with on_cancel:
// :stop. A producer of what a client asked for, a range of a table, is
// one: the client gone, it is of no more use.
type Ephemeral interface {
	StopAlone() bool
}

// Subscribe subscribes a consumer to the producer at To, under Tag, which
// HandleEvents gets with the events of this subscription, and Cancel
// takes; subscribing under a Tag in use cancels the subscription it had.
// Tags must be comparable.
//
// The consumer has at most Max events asked for and not received, and
// asks for more once they fall to Min, handling the events Max-Min at
// most at a time. Max is DefaultMax if zero, Min three quarters of Max.
//
// Partition is the partition to take of a producer with a Partition
// dispatcher, and is ignored of another.
//
// Ending is what the consumer does when its producer ends the
// subscription, exiting or cancelling it: by default, Permanent, it stops
// with the same reason.
//
// Held subscribes asking for none, until the consumer returns Release:
// consumers subscribing to a producer together, held, all subscribe
// before any asks, and none comes late to it done, as GenStage's manual
// demand has them.
type Subscribe struct {
	actor.Extension
	To        actor.Dest
	Tag       any
	Max, Min  int
	Partition int
	Ending    Ending
	Held      bool
}

// Ending is what a consumer does when its producer ends a subscription
// of it, as the cancel option of GenStage. It is told first, by
// HandleCancel, if it has one.
type Ending int

const (
	// Permanent stops the consumer with the same reason.
	Permanent Ending = iota
	// Transient stops it if the reason is not normal: a producer done
	// leaves it to go on, one failing stops it.
	Transient
	// Temporary leaves it to go on.
	Temporary
	// Resubscribe leaves it to go on, and subscribe again after a while,
	// to To, by its name: a producer restarted, or of a node back, is
	// another process, of another PID. It waits 100ms, twice as long each
	// time it fails again, up to 5s, and 100ms again once events come.
	Resubscribe
)

// stops reports whether a subscription ending with reason stops the
// consumer.
func (e Ending) stops(reason error) bool {
	switch e {
	case Transient:
		return !normal(reason)
	case Temporary, Resubscribe:
		return false
	}
	return true
}

// CancelHandler is a consumer, or a producer-consumer, told that its
// producer ended the subscription under tag with reason, exiting or
// cancelling it, before it does as the Ending of the subscription says.
type CancelHandler[S any] interface {
	HandleCancel(state S, tag any, reason error) (S, []actor.Effect)
}

// Release has a subscription made Held, under Tag, ask for Max events:
// the producer sends from then on, as to any. None held under Tag, it does
// nothing.
type Release struct {
	actor.Extension
	Tag any
}

// Cancel cancels the subscription of a consumer under Tag, telling its
// producer Reason. The consumer goes on.
type Cancel struct {
	actor.Extension
	Tag    any
	Reason error
}

// DefaultMax is the Max of a Subscribe without one.
const DefaultMax = 1000

// demand returns the Max and Min of x, its defaults applied, and whether
// they hold: 0 <= Min < Max.
func (x Subscribe) demand() (maxDemand, minDemand int, ok bool) {
	maxDemand, minDemand = x.Max, x.Min
	if maxDemand == 0 {
		maxDemand = DefaultMax
	}
	if minDemand == 0 {
		minDemand = maxDemand * 3 / 4
	}
	return maxDemand, minDemand, 0 <= minDemand && minDemand < maxDemand
}

// ErrBadDemand stops a consumer subscribing with a bad Max or Min.
var ErrBadDemand = errors.New("stage: Subscribe wants 0 <= Min < Max")

// BadEventsError stops a consumer sent events of another type than it
// takes.
type BadEventsError struct {
	Events any
}

func (e *BadEventsError) Error() string {
	return fmt.Sprintf("stage: events of type %T", e.Events)
}

// BadEffectError stops a stage returning an effect of stage that it does
// not perform: an Emit of a consumer, or of a type the producer does not
// emit, a Subscribe or a Cancel of a producer.
type BadEffectError struct {
	Effect actor.Effect
}

func (e *BadEffectError) Error() string {
	return fmt.Sprintf("stage: %T in this stage", e.Effect)
}

// isStageEffect reports whether e is an effect of stage, for the runtime
// never to see.
func isStageEffect(e actor.Effect) bool {
	switch e.(type) {
	case emitter, Done, Forward, Subscribe, Cancel, Release:
		return true
	}
	return false
}

// perform performs the effects of stage among effs on the sides the
// stage has, out emitting and in subscribing, nil if it has not: an Emit
// needs out, a Subscribe or a Cancel in, or they stop the stage with a
// BadEffectError. It appends the effects for the runtime to rest, in
// order.
func perform(rest, effs []actor.Effect, out *emitting, in *subscribing) []actor.Effect {
	for _, e := range effs {
		var err error
		rest, err = performOne(rest, e, out, in)
		if errors.Is(err, errEmitType) {
			err = &BadEffectError{Effect: e}
		}
		if err != nil {
			return append(rest, actor.Stop{Reason: err})
		}
	}
	return rest
}

// performOne performs e, if of stage, or else appends it to rest. Its
// error is errEmitType for an effect the stage may not perform, or why it
// failed.
func performOne(rest []actor.Effect, e actor.Effect, out *emitting, in *subscribing) ([]actor.Effect, error) {
	switch x := e.(type) {
	case emitter:
		if out == nil {
			return rest, errEmitType
		}
		return out.emitBatch(x.batch(), rest)
	case Done:
		if out == nil {
			return rest, errEmitType
		}
		out.done = true
		return rest, nil
	case Forward:
		if out == nil {
			return rest, errEmitType
		}
		out.forwarded = out.forwarded || out.holding
		out.holding = false
		return rest, nil
	case Subscribe:
		if in == nil {
			return rest, errEmitType
		}
		return append(rest, in.subscribe(x)...), nil
	case Cancel:
		if in == nil {
			return rest, errEmitType
		}
		return append(rest, in.cancel(x)...), nil
	case Release:
		if in == nil {
			return rest, errEmitType
		}
		return append(rest, in.release(x)...), nil
	}
	return append(rest, e), nil
}

// formatStatus formats the report of a stage terminating with the
// FormatStatus of its behaviour b, if it has one: see
// actor.StatusFormatter.
func formatStatus(b any, st actor.Status) actor.Status {
	if f, ok := b.(actor.StatusFormatter); ok {
		return f.FormatStatus(st)
	}
	return st
}

// terminate runs the Terminate of b, if it has one, without the effects
// of stage it returns.
func terminate[S any](b any, s S, reason error) []actor.Effect {
	t, ok := b.(Terminator[S])
	if !ok {
		return nil
	}
	return slices.DeleteFunc(t.Terminate(s, reason), isStageEffect)
}

// normal reports whether reason is that of an end without failure.
func normal(reason error) bool {
	return reason == nil || errors.Is(reason, proc.Normal)
}

// stops reports whether out stops the stage.
func stops(out []actor.Effect) bool {
	return slices.ContainsFunc(out, func(e actor.Effect) bool {
		_, ok := e.(actor.Stop)
		return ok
	})
}
