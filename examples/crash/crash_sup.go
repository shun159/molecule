package main

import (
	"context"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// startCrashSup starts the supervisor of the calc server. It restarts the
// server at most 3 times within 5 seconds; a 4th crash in that time means
// restarting does not help, and the supervisor gives up.
func startCrashSup(ctx context.Context, n *proc.Node) (proc.PID, error) {
	return supervisor.Start(ctx, n, supervisor.Spec{
		Name:      molecule.Local("crash_sup"),
		Strategy:  supervisor.OneForOne,
		Intensity: 3,
		Period:    5 * time.Second,
		Children: []supervisor.ChildSpec{{
			ID:      "calc",
			Start:   genserver.Child(CalcServer{}, molecule.WithName(calcName)),
			Restart: supervisor.Permanent,
		}},
	})
}
