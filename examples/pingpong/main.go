// Command pingpong runs a node that answers pings, and pings another node
// if told to: two programs talking through the distribution.
//
//	go run ./examples/pingpong -name b@localhost -listen 127.0.0.1:4371 \
//		-peer a@localhost=127.0.0.1:4370
//	go run ./examples/pingpong -name a@localhost -listen 127.0.0.1:4370 \
//		-peer b@localhost=127.0.0.1:4371 -ping b@localhost
//
// Each node runs pong_server, registered as pong. The node told to -ping
// runs pinger, which calls the pong of the other node by name every
// second. Stop and restart b, and a prints noconnection meanwhile, then
// pongs again.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// peers is the -peer flag: name=address, repeated.
type peers map[string]string

func (p peers) String() string { return "" }

func (p peers) Set(s string) error {
	name, addr, ok := strings.Cut(s, "=")
	if !ok {
		return flag.ErrHelp
	}
	p[name] = addr
	return nil
}

func (p peers) resolve(node string) (string, bool) {
	addr, ok := p[node]
	return addr, ok
}

func main() {
	name := flag.String("name", "a@localhost", "name of this node")
	listen := flag.String("listen", "127.0.0.1:4370", "address to accept other nodes on")
	cookie := flag.String("cookie", "molecule", "secret the nodes share")
	ping := flag.String("ping", "", "node to ping")
	others := peers{}
	flag.Var(others, "peer", "another node, as name=address; repeat for more")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n, d, err := startNode(*name, dist.Config{Listen: *listen, Cookie: *cookie, Resolve: others.resolve})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()
	log.Printf("%s: listening on %v", *name, d.Addr())
	if *ping != "" {
		n.Spawn(pinger(os.Stdout, *ping, time.Second))
	}
	<-ctx.Done()
}

// startNode starts a distributed node running the pong server.
func startNode(name string, cfg dist.Config) (*proc.Node, *dist.Dist, error) {
	n := proc.NewNode(name)
	d, err := dist.Start(n, cfg)
	if err != nil {
		return nil, nil, err
	}
	pong := PongServer{node: name}
	if _, err := genserver.Start(context.Background(), n, pong, gen.WithName(pongName)); err != nil {
		d.Stop()
		return nil, nil, err
	}
	return n, d, nil
}
