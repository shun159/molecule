package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
)

// kvringSup is the top supervisor of the application. The store is its
// only child; restarted, it serves on from the same table.
func kvringSup(node string, table *Table) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "kvring_store", Start: genserver.Child(Store{Node: node, Table: table}, actor.WithName(storeName))},
		},
	}
}
