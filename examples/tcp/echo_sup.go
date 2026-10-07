package main

import (
	"context"

	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
	"github.com/shun159/molecule/tcp"
)

// echo is the running application: its top supervisor and the listener
// under it.
type echo struct {
	sup      proc.PID
	listener *tcp.Listener
}

// startEcho starts the supervision tree of the application. The stats
// server comes first: the connections report to it, and with rest_for_one
// they are restarted along with it should it fail.
func startEcho(ctx context.Context, n *proc.Node, addr string) (*echo, error) {
	listener := tcp.NewListener(tcp.Spec{
		Addr:    addr,
		Handler: protocolStartFunc(statsRef),
	})
	sup, err := supervisor.Start(ctx, n, supervisor.Spec{
		Strategy:  supervisor.RestForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			statsChildSpec("echo_stats"),
			listener.ChildSpec("echo_listener"),
		},
	})
	if err != nil {
		return nil, err
	}
	return &echo{sup: sup, listener: listener}, nil
}
