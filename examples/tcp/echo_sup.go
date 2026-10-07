package main

import (
	"context"
	"net"

	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

// acceptors is how many processes accept connections at once.
const acceptors = 4

// echo is the running application: its top supervisor and the address
// it listens on.
type echo struct {
	sup  proc.PID
	addr net.Addr
}

// startEcho starts the supervision tree of the application. With
// rest_for_one, a child failing restarts those after it: the stats server
// takes everything down with it, as the connections report to it; the
// connection supervisor takes the listener, whose acceptors start
// connections under it.
func startEcho(ctx context.Context, n *proc.Node, addr string) (*echo, error) {
	sup, err := supervisor.Start(ctx, n, supervisor.Spec{
		Strategy:  supervisor.RestForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			statsChildSpec("echo_stats"),
			connsChildSpec("echo_conns"),
			listenerChildSpec("echo_listener", addr, acceptors),
		},
	})
	if err != nil {
		return nil, err
	}
	a, err := listenerAddr(ctx, n)
	if err != nil {
		supervisor.Stop(ctx, n, sup)
		return nil, err
	}
	return &echo{sup: sup, addr: a}, nil
}
