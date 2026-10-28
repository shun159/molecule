package main

import (
	"fmt"
	"io"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/behaviours/supervisor"
)

// config is what a node runs: the file it tails, its parsers, and its
// partition of the reducers, one a node.
type config struct {
	file      string
	fromStart bool
	parsers   int
	nodes     []string // all of them, this one among them, in order
	partition int      // of this node
	window    time.Duration
	top       int
	out       io.Writer
}

var tailName = actor.Local("logstats_tail")

func parseName(i int) actor.Local { return actor.Local(fmt.Sprintf("logstats_parse_%d", i)) }

const reduceName = actor.Local("logstats_reduce")

// logstatsSup is the top supervisor of a node: its tail, its parsers, and
// its reducer, each on its own. They find each other by name, and
// subscribe again when one is restarted, or a node is back.
func logstatsSup(c config) supervisor.Spec {
	children := []supervisor.ChildSpec{{
		ID:    "logstats_tail",
		Start: stage.Child(stage.Produce(Tail{Path: c.file, Poll: 100 * time.Millisecond, FromStart: c.fromStart}), actor.WithName(tailName)),
	}}
	for i := range c.parsers {
		parse := Parse{From: tailName, Partitions: len(c.nodes)}
		children = append(children, supervisor.ChildSpec{
			ID:    string(parseName(i)),
			Start: stage.Child(stage.ProduceConsume(parse), actor.WithName(parseName(i))),
		})
	}
	reduce := Reduce{Partition: c.partition, From: c.parsersOfAll(), Every: c.window, Top: c.top, Out: c.out}
	children = append(children, supervisor.ChildSpec{
		ID:    string(reduceName),
		Start: stage.Child(stage.Consume(reduce), actor.WithName(reduceName)),
	})
	return supervisor.Spec{Strategy: supervisor.OneForOne, Intensity: 5, Children: children}
}

// parsersOfAll is the parsers of every node, each having as many as this
// one.
func (c config) parsersOfAll() []actor.Dest {
	var all []actor.Dest
	for _, node := range c.nodes {
		for i := range c.parsers {
			all = append(all, parseName(i).At(node))
		}
	}
	return all
}
