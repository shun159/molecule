// Command logstats counts the lines of log files by key over nodes, by
// windows of time: each node tails its file, parses its lines into keys
// and values, and sends each record to the node of its key, which counts
// and sums the records of its keys, and writes them out at the end of
// every window. A stream with no end, which may lose some records: a
// node gone, its keys are not counted until it is back.
//
//	logstats_sup (one_for_one), on every node
//	├── logstats_tail     producer: the lines of the file, as written
//	├── logstats_parse_i  producer-consumers: records, partitioned by key
//	└── logstats_reduce   consumer: its partition, of all the nodes
//
// Each file holds one "module". The lines are "key value", or "key" alone
// for a value of 1:
//
//	go run ./examples/logstats -name a@localhost -file a.log -gen 20000
//	go run ./examples/logstats -name b@localhost -file b.log -gen 20000
//
// Every window, each node writes its keys, the most counted first. Stop
// b: a goes on, the keys of b uncounted, until b is back. So are the
// records of a node parsed before the reducers of the others have
// subscribed to it, as it starts.
package main

import (
	"context"
	"flag"
	"log"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

func main() {
	name, addrs, gen, c := parseFlags()
	n := proc.NewNode(name)
	d, err := startDist(n, addrs)
	if err != nil {
		log.Fatal(err)
	}
	defer d.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if gen > 0 {
		go func() {
			if err := generate(ctx, c.file, gen); err != nil {
				log.Print(err)
			}
		}()
	}
	app, err := startLogstats(context.Background(), n, c)
	if err != nil {
		log.Fatal(err)
	}
	select {
	case <-ctx.Done():
		app.Stop(context.Background())
	case <-app.Done(): // the tree gave up
		log.Print(app.Err())
	}
}

// parseFlags returns the name of this node, the addresses of all, the
// lines a second to generate, and what the node runs.
func parseFlags() (string, map[string]string, int, config) {
	name := flag.String("name", "a@localhost", "name of this node, one of -nodes")
	nodes := flag.String("nodes", "a@localhost=127.0.0.1:4390,b@localhost=127.0.0.1:4391",
		"the nodes, as name=address, comma separated")
	file := flag.String("file", "logstats.log", "the file to tail")
	fromStart := flag.Bool("from-start", false, "read the file from its start, not its end")
	parsers := flag.Int("parsers", 2, "parsers on each node, the same on all")
	window := flag.Duration("window", 10*time.Second, "the window of the counts")
	top := flag.Int("top", 5, "keys written of each window")
	gen := flag.Int("gen", 0, "lines a second to append to the file, to try it")
	flag.Parse()

	addrs := parseNodes(*nodes)
	all := slices.Sorted(maps.Keys(addrs))
	partition := slices.Index(all, *name)
	if partition < 0 {
		log.Fatalf("%s is not in -nodes", *name)
	}
	c := config{file: *file, fromStart: *fromStart, parsers: *parsers, nodes: all, partition: partition,
		window: *window, top: *top, out: os.Stdout}
	return *name, addrs, *gen, c
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
		Cookie:   "logstats",
		Resolve:  func(node string) (string, bool) { a, ok := addrs[node]; return a, ok },
		TickTime: 4 * time.Second,
	})
}
