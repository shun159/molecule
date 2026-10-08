package main

import (
	"fmt"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/genserver"
)

// PongServer is a gen_server answering pings, registered as pong on each
// node. The other node reaches it by name: molecule.Remote{Node, "pong"}.
type PongServer struct {
	genserver.Default[int] // the count of pings
	node                   string
}

var pongName = molecule.Local("pong")

func (s PongServer) HandleCall(n int, ping string, from genserver.From[string]) (int, []molecule.Effect) {
	n++
	return n, molecule.Do(from.Reply(fmt.Sprintf("pong #%d from %s to %q", n, s.node, ping)))
}
