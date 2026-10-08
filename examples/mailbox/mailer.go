package main

import (
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// ping spawns a mailer that casts pass Pongs to receiver, then exits: the
// be ping(receiver, pass) of the Pony example. A mailer has no state and
// handles no messages, so it is a plain process rather than a behaviour.
func ping(n *proc.Node, receiver genserver.Ref[Wait, int, Pong], pass int) proc.PID {
	return n.Spawn(func(s *proc.Self) error {
		for range pass {
			receiver.Cast(s, Pong{})
		}
		return nil
	})
}
