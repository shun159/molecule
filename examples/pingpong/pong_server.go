package main

import (
	"fmt"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
)

// PongServer is a gen_server answering pings, registered as pong on each
// node. The other node reaches it by name: actor.Remote{Node, "pong"}.
type PongServer struct {
	genserver.Default[int] // the count of pings
	node                   string
}

var pongName = actor.Local("pong")

func (s PongServer) HandleCall(n int, ping string, from genserver.From[string]) (int, []actor.Effect) {
	n++
	return n, actor.Do(from.Reply(fmt.Sprintf("pong #%d from %s to %q", n, s.node, ping)))
}
