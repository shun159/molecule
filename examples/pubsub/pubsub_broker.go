package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/dist/pg"
)

// scope is the pg scope of the topics, shared by the nodes: a topic is a
// group of it, its subscribers its members, on any node.
var scope = actor.Local("pubsub_pg")

// Published is what a subscriber gets: Text, published on Topic from the
// node From.
type Published struct {
	Topic string
	From  string
	Text  string
}

func (p Published) String() string { return "[" + p.Topic + "] " + p.Text + " (from " + p.From + ")" }

func init() { dist.Register(Published{}) }

// brokerName is the broker of a node.
var brokerName = actor.Local("pubsub_broker")

// Broker publishes on the node it runs on: it has the scope send what is
// published to every subscriber of its topic, on whatever node, once.
type Broker struct {
	genserver.Default[struct{}]
	Node string
}

// publish is the cast publishing Text on Topic.
type publish struct{ Topic, Text string }

func (b Broker) HandleCast(s struct{}, m publish) (struct{}, []actor.Effect) {
	msg := Published{Topic: m.Topic, From: b.Node, Text: m.Text}
	return s, actor.Do(pg.SendEffect(scope, m.Topic, msg, zeroPID))
}

// Publish publishes text on topic, from the node of caller: to those
// subscribed now, at most once each, and to none of a node cut off.
func Publish(caller actor.Caller, topic, text string) {
	genserver.NewRef[genserver.None, genserver.None, publish](brokerName).Cast(caller, publish{topic, text})
}
