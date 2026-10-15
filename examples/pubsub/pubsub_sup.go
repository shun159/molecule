package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/dist/pg"
)

// pubsubSup is the top supervisor of the application: the scope first, as
// the subscribers, its members, are restarted with it, to join anew.
func pubsubSup(node string) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.RestForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "pubsub_pg", Start: pg.Child(scope)},
			{ID: "pubsub_broker", Start: genserver.Child(Broker{Node: node}, actor.WithName(brokerName))},
			{ID: "pubsub_subs", Start: supervisor.DynamicChild(supervisor.DynamicSpec{Name: subsName}), Type: supervisor.Supervisor},
		},
	}
}
