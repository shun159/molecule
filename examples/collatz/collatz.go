package main

import (
	"context"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// span is the numbers from Lo to Hi-1.
type span struct{ Lo, Hi int }

// chain is a number and the steps of its chain down to 1.
type chain struct{ Start, Steps int }

// Steps returns the steps of the Collatz chain of n down to 1.
func Steps(n int) int {
	steps := 0
	for n != 1 {
		if n%2 == 0 {
			n /= 2
		} else {
			n = 3*n + 1
		}
		steps++
	}
	return steps
}

// Longest returns the number of s with the longest chain, the first of
// those as long.
func Longest(s span) chain {
	best := chain{Start: s.Lo, Steps: -1}
	for n := s.Lo; n < s.Hi; n++ {
		if steps := Steps(n); steps > best.Steps {
			best = chain{Start: n, Steps: steps}
		}
	}
	return best
}

// Run finds the number below n with the longest chain, with the given
// workers taking ranges of chunk numbers, on node. The stages are all
// subscribed before the ranges start.
func Run(ctx context.Context, node *proc.Node, n, chunk, workers int) (chain, error) {
	src, err := stage.Start(ctx, node, stage.Produce(Ranges{N: n, Chunk: chunk}))
	if err != nil {
		return chain{}, err
	}
	var from []actor.Dest
	for range workers {
		w, err := stage.Start(ctx, node, stage.ProduceConsume(Worker{From: src}))
		if err != nil {
			return chain{}, err
		}
		from = append(from, w)
	}
	done := node.Alias()
	defer done.Release()
	best, err := stage.Start(ctx, node, stage.Consume(Best{From: from, Done: actor.From{Tag: done.Ref}}))
	if err != nil {
		return chain{}, err
	}
	watch, stop := node.Watch(ctx, best)
	defer stop()
	node.Send(src, start{})

	select {
	case m := <-done.C:
		return m.Msg.(chain), nil
	case <-watch.Done():
		return chain{}, context.Cause(watch) // Best gone without telling
	}
}
