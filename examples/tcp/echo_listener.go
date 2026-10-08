package main

import (
	"context"
	"net"
	"sync"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/net/gentcp"
	"github.com/shun159/molecule/proc"
)

// The listener owns the listening socket, and runs the acceptors linked
// to it: if one of them fails, they all go with the socket, and echo_sup
// starts them again. It is a plain process, like a proc_lib special
// process, rather than a gen_server: it does nothing but own the socket,
// which only a process can, and wait to be stopped.

// listener is a started listener, as its child spec makes it: the address
// it listens on is there once it has started.
type listener struct {
	mu   sync.Mutex
	addr net.Addr
}

func (l *listener) childSpec(id, addr string, acceptors int) supervisor.ChildSpec {
	return supervisor.ChildSpec{
		ID: id,
		Start: supervisor.StartFunc(func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
			return parent.StartLink(ctx, func(self *proc.Self) error {
				return l.listen(self, addr, acceptors)
			})
		}),
	}
}

// Addr returns the address the listener listens on.
func (l *listener) Addr() net.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.addr
}

func (l *listener) listen(self *proc.Self, addr string, acceptors int) error {
	ls, err := gentcp.Listen(context.Background(), self, addr, gentcp.Options{})
	if err != nil {
		self.InitAck(err)
		return err
	}
	l.mu.Lock()
	l.addr = ls.Addr()
	l.mu.Unlock()
	for range acceptors {
		self.SpawnLink(func(a *proc.Self) error { return accept(a, ls) })
	}
	self.InitAck(nil)
	for {
		if _, err := self.Receive(context.Background()); err != nil {
			return err
		}
	}
}
