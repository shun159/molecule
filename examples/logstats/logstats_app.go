package main

import (
	"context"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startLogstats starts the application on n: its supervision tree.
func startLogstats(ctx context.Context, n *proc.Node, c config) (*application.Running, error) {
	return application.Start(ctx, n, application.App{
		Name:  "logstats",
		Start: supervisor.Child(logstatsSup(c)),
	})
}
