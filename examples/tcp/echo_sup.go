package main

import (
	"context"
	"net"

	"github.com/shun159/molecule/application"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// acceptors is how many processes accept connections at once.
const acceptors = 4

// echoSup is the top supervisor of the application. With rest_for_one, a
// child failing restarts those after it: the stats server takes
// everything down with it, as the connections report to it; the connection
// supervisor takes the listener, whose acceptors start connections under
// it.
func echoSup(addr string, l *listener) supervisor.Spec {
	return supervisor.Spec{
		Strategy:  supervisor.RestForOne,
		Intensity: 5,
		Children: []supervisor.ChildSpec{
			statsChildSpec("echo_stats"),
			connsChildSpec("echo_conns"),
			l.childSpec("echo_listener", addr, acceptors),
		},
	}
}

// startEcho starts the application, and returns the address it listens
// on, which the listener has once started.
func startEcho(ctx context.Context, n *proc.Node, addr string) (*application.Running, net.Addr, error) {
	l := &listener{}
	r, err := application.Start(ctx, n, application.App{Name: "echo", Start: supervisor.Child(echoSup(addr, l))})
	if err != nil {
		return nil, nil, err
	}
	return r, l.Addr(), nil
}
