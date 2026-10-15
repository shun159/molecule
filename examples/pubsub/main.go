// Command pubsub publishes messages on topics to their subscribers, over
// nodes, by process groups: a topic is a group of a pg scope the nodes
// share, its subscribers the processes in it, wherever they run.
//
//	pubsub_sup (rest_for_one)
//	├── pubsub_pg      the pg scope, shared with the other nodes
//	├── pubsub_broker  gen_server publishing on the topics
//	└── pubsub_subs    dynamic supervisor of the subscribers of this node
//
// Each file holds one "module": pubsub_app.go is the application,
// pubsub_sup.go the supervisor, pubsub_broker.go and pubsub_subscriber.go
// the gen_servers and their API, pubsub_console.go the console.
//
//	go run ./examples/pubsub -name a@localhost
//	go run ./examples/pubsub -name b@localhost
//
// Then, on b:
//
//	sub news
//
// and on a:
//
//	pub news hello
//
// b writes "[news] hello (from a@localhost)". A message goes to those
// subscribed as it is published, at most once: a node down then, or cut
// off, misses it; nothing is kept. examples of quorum has the other kind,
// a topic replicated, which subscribers read from where they left off.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

func main() {
	name := flag.String("name", "a@localhost", "name of this node, one of -nodes")
	nodes := flag.String("nodes", "a@localhost=127.0.0.1:4390,b@localhost=127.0.0.1:4391,c@localhost=127.0.0.1:4392",
		"the nodes, as name=address, comma separated")
	flag.Parse()

	addrs := parseNodes(*nodes)
	if _, ok := addrs[*name]; !ok {
		log.Fatalf("%s is not in -nodes", *name)
	}
	n := proc.NewNode(*name)
	d, err := dist.Start(n, dist.Config{
		Listen:   addrs[*name],
		Cookie:   "pubsub",
		Resolve:  func(node string) (string, bool) { a, ok := addrs[node]; return a, ok },
		TickTime: 4 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()
	for other := range addrs {
		if other != *name {
			go keepConnected(d, other)
		}
	}

	app, err := startPubsub(context.Background(), n)
	if err != nil {
		log.Fatal(err)
	}
	go newConsole(n, os.Stdout).run(os.Stdin)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		app.Stop(context.Background())
	case <-app.Done(): // the tree gave up
		log.Print(app.Err())
	}
}

// keepConnected connects to node, again once lost, for the scope of this
// node to meet the scope there: dist connects only to send, and the
// scopes meet as nodes come up.
func keepConnected(d *dist.Dist, node string) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		d.Connect(ctx, node)
		cancel()
		time.Sleep(time.Second)
	}
}

// parseNodes parses name=address pairs, comma separated.
func parseNodes(s string) map[string]string {
	addrs := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		name, addr, _ := strings.Cut(pair, "=")
		addrs[name] = addr
	}
	return addrs
}
