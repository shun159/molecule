package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startPubsub starts the application on n: its supervision tree.
func startPubsub(ctx context.Context, n *proc.Node) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "pubsub",
		Start: supervisor.Child(pubsubSup(n.Name())),
	})
}
