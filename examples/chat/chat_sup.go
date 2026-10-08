package main

import (
	"context"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/net/gentcp"
	"github.com/shun159/molecule/net/gentcpacceptor"
	"github.com/shun159/molecule/pg"
	"github.com/shun159/molecule/proc"
)

// scope is the pg scope of the chat, and lobby the group everyone is in.
var (
	scope = molecule.Local("chat_pg")
	lobby = "lobby"
)

// chat is the running application.
type chat struct {
	sup      proc.PID
	listener *gentcpacceptor.Listener
}

// startChat starts the supervision tree. The scope comes first: if it
// fails, the connections, whose membership it held, are restarted too.
func startChat(ctx context.Context, n *proc.Node, addr string) (*chat, error) {
	// The sockets cut lines, of a reasonable length.
	listener := gentcpacceptor.NewListener(gentcpacceptor.Spec{
		Addr:    addr,
		Options: gentcp.Options{Packet: gentcp.Line, PacketSize: 1024},
	}, ChatProtocol{})
	sup, err := supervisor.Start(ctx, n, supervisor.Spec{
		Strategy: supervisor.RestForOne,
		Children: []supervisor.ChildSpec{
			{ID: "chat_pg", Start: pg.StartLinkFunc(scope)},
			listener.ChildSpec("chat_listener"),
		},
	})
	if err != nil {
		return nil, err
	}
	return &chat{sup: sup, listener: listener}, nil
}
