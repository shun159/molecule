package main

import (
	"context"

	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/net/gentcp"
	"github.com/shun159/molecule/proc"
)

// accept is an acceptor: it accepts a connection, starts an echo_protocol
// under echo_conns, and gives it the socket, like the acceptors of Ranch.
// The socket is passive until the protocol asks for data, so nothing is
// read while it changes hands.
func accept(self *proc.Self, ls *gentcp.ListenSocket) error {
	self.SetLabel("echo_acceptor")
	ctx := context.Background()
	for {
		sock, err := ls.Accept(ctx, self)
		if err != nil {
			return err
		}
		pid, err := supervisor.StartChild(ctx, self, connsName, protocolChildSpec())
		if err != nil {
			sock.Close(ctx, self)
			continue
		}
		if err := sock.ControllingProcess(ctx, self, pid); err != nil {
			// The peer is gone already.
			supervisor.TerminateChild(ctx, self, connsName, pid)
			continue
		}
		genserver.RefFor(EchoProtocol{}, pid).Cast(self, sock)
	}
}
