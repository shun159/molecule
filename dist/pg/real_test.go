package pg_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/dist/pg"
	"github.com/shun159/molecule/proc"
)

// TestOverDist has the scopes of two nodes, connected by dist, share their
// members, and lose those of the other when disconnected.
func TestOverDist(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	addrs := map[string]string{}
	resolve := func(node string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		a, ok := addrs[node]
		return a, ok
	}
	var nodes []*proc.Node
	var dists []*dist.Dist
	var members []proc.PID
	for _, name := range []string{"a@test", "b@test"} {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "c", Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		defer d.Stop()
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		if _, err := pg.Start(ctx, n, molecule.Local("pg")); err != nil {
			t.Fatal(err)
		}
		m := n.Spawn(func(s *proc.Self) error {
			_, err := s.Receive(context.Background())
			return err
		})
		if err := pg.Join(ctx, n, molecule.Local("pg"), "g", m); err != nil {
			t.Fatal(err)
		}
		nodes, dists, members = append(nodes, n), append(dists, d), append(members, m)
	}
	count := func(n *proc.Node) int {
		pids, err := pg.Members(ctx, n, molecule.Local("pg"), "g")
		if err != nil {
			t.Fatal(err)
		}
		return len(pids)
	}
	eventually := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	if err := dists[0].Connect(ctx, "b@test"); err != nil {
		t.Fatal(err)
	}
	eventually("both members on both nodes", func() bool { return count(nodes[0]) == 2 && count(nodes[1]) == 2 })
	dists[0].Disconnect("b@test")
	eventually("each node back to its own", func() bool { return count(nodes[0]) == 1 && count(nodes[1]) == 1 })
}
