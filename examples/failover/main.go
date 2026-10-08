// Command failover runs a worker on each of two nodes: the primary counts,
// and sends each count to the standby, which takes over when the primary
// is gone.
//
//	go run ./examples/failover -name b@localhost -listen 127.0.0.1:4371 \
//		-peer a@localhost=127.0.0.1:4370 -primary
//	go run ./examples/failover -name a@localhost -listen 127.0.0.1:4370 \
//		-peer b@localhost=127.0.0.1:4371
//
// Stop b: a, monitoring the worker of b across the nodes, learns of it
// with noconnection, and counts on from the last count it got.
//
// worker.go is the worker, a gen_server; console.go prints for it.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

func init() { dist.Register(checkpoint{}) }

func main() {
	name := flag.String("name", "a@localhost", "name of this node")
	listen := flag.String("listen", "127.0.0.1:4370", "address to accept the other node on")
	peer := flag.String("peer", "b@localhost=127.0.0.1:4371", "the other node, as name=address")
	primary := flag.Bool("primary", false, "count, rather than stand by")
	flag.Parse()
	peerName, peerAddr, _ := strings.Cut(*peer, "=")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n := proc.NewNode(*name)
	d, err := dist.Start(n, dist.Config{
		Listen:   *listen,
		Cookie:   "failover",
		Resolve:  func(node string) (string, bool) { return peerAddr, node == peerName },
		TickTime: 4 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()
	n.Register(consoleName, n.Spawn(console(os.Stdout)))
	w := Worker{Node: *name, Peer: peerName, Primary: *primary, Every: 500 * time.Millisecond}
	if _, err := genserver.Start(ctx, n, w, molecule.WithName(workerName)); err != nil {
		log.Fatal(err)
	}
	<-ctx.Done()
}
