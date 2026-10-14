package main

import (
	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/kvs"
)

// kvringSup is the top supervisor of the application: the table in dir,
// then the store serving it. They restart together: the store stops on a
// write failed, after which the table must be opened again.
func kvringSup(node, dir string) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.OneForAll,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			{ID: "kvring_table", Start: kvs.Child(dir, kvs.Options[string, string]{})},
			{ID: "kvring_store", Start: genserver.Child(Store{Node: node, Dir: dir}, actor.WithName(storeName))},
		},
	}
}
