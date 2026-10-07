package main

import (
	"fmt"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// PongServer is a gen_server answering pings, registered as pong on each
// node. The other node reaches it by name: gen.Remote{Node, "pong"}.
type PongServer struct{ node string }

var pongName = gen.Local("pong")

// pongRef is how to call the pong server of node.
func pongRef(node string) genserver.Ref[string, string, struct{}] {
	return genserver.NewRef[string, string, struct{}](gen.Remote{Node: node, Name: string(pongName)})
}

func (PongServer) Init(proc.PID) (int, []gen.Effect, error) { return 0, nil, nil }

func (s PongServer) HandleCall(n int, ping string, from genserver.From[string]) (int, []gen.Effect) {
	n++
	return n, gen.Do(from.Reply(fmt.Sprintf("pong #%d from %s to %q", n, s.node, ping)))
}

func (PongServer) HandleCast(n int, _ struct{}) (int, []gen.Effect) { return n, nil }
