// Command kvring is a table of keys and values over nodes, partitioned by
// consistent hashing: each node keeps the keys the ring gives it, in a kvs
// table, and a client asks the node of the key.
//
//	kvring_sup (one_for_all)
//	├── kvring_table  the table of this node, on disk
//	└── kvring_store  gen_server of the keys of this node, in the table
//
// Each file holds one "module": kvring_app.go is the application,
// kvring_sup.go the supervisor, kvring_store.go the gen_server and its
// API, kvring_ring.go the ring, kvring_console.go the console.
//
//	go run ./examples/kvring -name a@localhost
//	go run ./examples/kvring -name b@localhost
//	go run ./examples/kvring -name c@localhost
//
// Then, on any of them:
//
//	put color blue
//	ok (c@localhost)
//	get color
//	blue (c@localhost)
//
// Stop c: the keys of a and b are still there, those of c are not until it
// is back, from its table. There is no replication.
package main

import (
	"context"
	"flag"
	"log"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// vnodes is the points of each node on the ring.
const vnodes = 64

func main() {
	name := flag.String("name", "a@localhost", "name of this node, one of -nodes")
	nodes := flag.String("nodes", "a@localhost=127.0.0.1:4380,b@localhost=127.0.0.1:4381,c@localhost=127.0.0.1:4382",
		"the nodes of the ring, as name=address, comma separated")
	dir := flag.String("dir", "kvring-data", "directory of the tables, one per node")
	flag.Parse()

	addrs := parseNodes(*nodes)
	if _, ok := addrs[*name]; !ok {
		log.Fatalf("%s is not in -nodes", *name)
	}
	ring := NewRing(slices.Sorted(maps.Keys(addrs)), vnodes)

	n := proc.NewNode(*name)
	d, err := startDist(n, addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()

	app, err := startKvring(context.Background(), n, filepath.Join(*dir, *name))
	if err != nil {
		log.Fatal(err)
	}
	go console{n: n, ring: ring, out: os.Stdout}.run(os.Stdin)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		app.Stop(context.Background())
	case <-app.Done(): // the tree gave up
		log.Print(app.Err())
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

// startDist connects n to the other nodes, at addrs, listening at its own.
func startDist(n *proc.Node, addrs map[string]string) (*dist.Dist, error) {
	return dist.Start(n, dist.Config{
		Listen:   addrs[n.Name()],
		Cookie:   "kvring",
		Resolve:  func(node string) (string, bool) { a, ok := addrs[node]; return a, ok },
		TickTime: 4 * time.Second,
	})
}
