// Command raft runs a cluster of three Raft servers, each on a node of its
// own, connected by dist on loopback, in one program: it proposes
// commands, stops the leader, and proposes more to the new one.
//
//	go run ./examples/raft
//
// raft_server.go is the server, a gen_statem. raft_test.go tests the rules
// of the paper as calls of its pure functions. raft_sim_test.go runs
// clusters of five in gensim through partitions, crashes, restarts and
// lost messages, checking the safety of Raft after every step, for a
// hundred seeds; more search further:
//
//	RAFT_SEEDS=2000 go test ./examples/raft
//
// With the commit rule of figure 8 of the paper broken, seed 1077 finds a
// leader lacking an entry committed: the run replays with that seed.
package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

func init() {
	for _, v := range []any{RequestVote{}, Vote{}, AppendEntries{}, Appended{}, Propose{}, GetStatus{}, Status{}} {
		gob.Register(v)
	}
}

type member struct {
	node *proc.Node
	dist *dist.Dist
	pid  proc.PID
}

func main() {
	names := []string{"a@localhost", "b@localhost", "c@localhost"}
	addrs := map[string]string{}
	resolve := func(node string) (string, bool) { a, ok := addrs[node]; return a, ok }
	cluster := map[string]*member{}
	for i, name := range names {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "raft", Resolve: resolve})
		if err != nil {
			log.Fatal(err)
		}
		addrs[name] = d.Addr().String()
		srv := Server{
			Node:            name,
			Peers:           slices.DeleteFunc(slices.Clone(names), func(p string) bool { return p == name }),
			Seed:            uint64(i),
			ElectionTimeout: 150 * time.Millisecond,
			Heartbeat:       50 * time.Millisecond,
		}
		pid, err := genstatem.Start(context.Background(), n, srv, gen.WithName(gen.Local(raftName)))
		if err != nil {
			log.Fatal(err)
		}
		cluster[name] = &member{n, d, pid}
	}
	client := cluster[names[0]].node

	leader := waitLeader(client, names)
	fmt.Println("leader:", leader)
	for i := range 3 {
		propose(client, leader, fmt.Sprintf("set x %d", i))
	}
	time.Sleep(300 * time.Millisecond)
	show(client, names)

	fmt.Println("stopping", leader)
	m := cluster[leader]
	genstatem.Stop(context.Background(), m.node, m.pid)
	m.dist.Stop()
	alive := slices.DeleteFunc(slices.Clone(names), func(n string) bool { return n == leader })
	client = cluster[alive[0]].node

	leader = waitLeader(client, alive)
	fmt.Println("new leader:", leader)
	propose(client, leader, "set y 1")
	time.Sleep(300 * time.Millisecond)
	show(client, alive)
}

func status(client *proc.Node, node string) (Status, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := gen.Call(ctx, client, gen.Remote{Node: node, Name: raftName}, GetStatus{})
	if err != nil {
		return Status{}, err
	}
	return v.(Status), nil
}

// waitLeader waits for a leader among nodes.
func waitLeader(client *proc.Node, nodes []string) string {
	for {
		for _, n := range nodes {
			if st, err := status(client, n); err == nil && st.Role == Leader {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func propose(client *proc.Node, leader, cmd string) {
	gen.SendCast(client, gen.Remote{Node: leader, Name: raftName}, Propose{cmd})
}

func show(client *proc.Node, nodes []string) {
	for _, n := range nodes {
		st, err := status(client, n)
		if err != nil {
			fmt.Printf("  %s: %v\n", n, err)
			continue
		}
		fmt.Printf("  %s: %-9v term %d, committed %v\n", n, st.Role, st.Term, st.Committed)
	}
}
