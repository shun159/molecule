package main

import (
	"context"
	"net"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

// The listener owns the listening socket, and runs the acceptors linked
// to it: if one of them fails, they all go with the socket, and echo_sup
// starts them again. It is a plain process, like a proc_lib special
// process, rather than a gen_server: it waits only to be asked its address.

var listenerName = gen.Local("echo_listener")

// getAddr asks the listener its address.
type getAddr struct{}

func listenerChildSpec(id, addr string, acceptors int) supervisor.ChildSpec {
	return supervisor.ChildSpec{
		ID: id,
		Start: func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
			return parent.StartLink(ctx, func(self *proc.Self) error {
				return listen(self, addr, acceptors)
			})
		},
	}
}

// listenerAddr returns the address the listener listens on.
func listenerAddr(ctx context.Context, caller gen.Caller) (net.Addr, error) {
	a, err := gen.Call(ctx, caller, listenerName, getAddr{})
	if err != nil {
		return nil, err
	}
	return a.(net.Addr), nil
}

func listen(self *proc.Self, addr string, acceptors int) error {
	ls, err := gentcp.Listen(context.Background(), self, addr, gentcp.Options{})
	if err == nil {
		err = self.Node().Register(string(listenerName), self.PID())
	}
	if err != nil {
		self.InitAck(err)
		return err
	}
	for range acceptors {
		self.SpawnLink(func(a *proc.Self) error { return accept(a, ls) })
	}
	self.InitAck(nil)

	for {
		msg, err := self.Receive(context.Background())
		if err != nil {
			return err
		}
		if call, ok := msg.(gen.CallMsg); ok {
			if _, ok := call.Req.(getAddr); ok {
				gen.SendReply(self, call.From, ls.Addr())
			}
		}
	}
}
