package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startKvring starts the application on n: its supervision tree, serving
// the part of the table kept in dir.
func startKvring(ctx context.Context, n *proc.Node, dir string) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "kvring",
		Start: supervisor.Child(kvringSup(n.Name(), dir)),
	})
}
