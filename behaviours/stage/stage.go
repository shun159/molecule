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

// Emit sends Events to the consumer of a producer, as far as it demands
// them; the rest wait in the producer, sent first as demand comes. The
// Events are not to be written once emitted.
type Emit[T any] struct {
	actor.Extension
	Events []T
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
// A subscription ending, its producer exiting or cancelling it, stops the
// consumer with the same reason.
type Subscribe struct {
	actor.Extension
	To       actor.Dest
	Tag      any
	Max, Min int
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

// Errors stopping a stage.
var (
	ErrBadDemand   = errors.New("stage: Subscribe wants 0 <= Min < Max")
	ErrHasConsumer = errors.New("stage: the producer has a consumer already")
)

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
	case emitter, Subscribe, Cancel:
		return true
	}
	return false
}

// stopBad stops a stage with a BadEffectError for e.
func stopBad(e actor.Effect) actor.Effect {
	return actor.Stop{Reason: &BadEffectError{Effect: e}}
}

// stops reports whether out stops the stage.
func stops(out []actor.Effect) bool {
	return slices.ContainsFunc(out, func(e actor.Effect) bool {
		_, ok := e.(actor.Stop)
		return ok
	})
}
