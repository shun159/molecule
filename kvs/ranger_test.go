package kvs_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

func init() { stage.Register[kvs.Entry[int, int]]() }

// ordered opens an OrderedSet of keys 0 to n-1, of values their squares.
func ordered(t *testing.T, n int) *kvs.Table[int, int] {
	t.Helper()
	tab := open(t, t.TempDir(), kvs.Options[int, int]{Type: kvs.OrderedSet, WriteConcurrency: true, Shards: 4, Sync: kvs.SyncNone})
	for k := range n {
		if err := tab.Put(k, k*k); err != nil {
			t.Fatal(err)
		}
	}
	return tab
}

// ranged pulls the entries of r, of n, as a client of c, with a Max of max.
func ranged(t *testing.T, ctx context.Context, n, c *proc.Node, r kvs.Ranger[int, int], maxDemand int) ([]int, error) {
	t.Helper()
	pid, err := stage.Start(ctx, n, stage.Produce(r))
	if err != nil {
		return nil, err
	}
	var keys []int
	for e, err := range stage.Pull[kvs.Entry[int, int]](ctx, c, stage.Subscribe{To: pid, Max: maxDemand}) {
		if err != nil {
			return keys, err
		}
		if e.Value != e.Key*e.Key {
			t.Fatalf("%d: %d", e.Key, e.Value)
		}
		keys = append(keys, e.Key)
	}
	return keys, nil
}

func span(lo, hi int) []int {
	var ks []int
	for k := lo; k < hi; k++ {
		ks = append(ks, k)
	}
	return ks
}

// A Ranger emits the range of its snapshot, batch after batch, the table
// written meanwhile, however much is asked for at a time.
func TestRanger(t *testing.T) {
	tab := ordered(t, 10000)
	snap := tab.Snapshot()
	var wg sync.WaitGroup
	wg.Go(func() {
		for k := range 10000 {
			tab.Put(k, -1) // not in the snapshot
		}
	})
	defer wg.Wait()
	n := proc.NewNode("")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, maxDemand := range []int{1, 7, 1000} {
		keys, err := ranged(t, ctx, n, n, kvs.Ranger[int, int]{Snapshot: snap, Lo: 1234, Hi: 5678}, maxDemand)
		if err != nil || !slices.Equal(keys, span(1234, 5678)) {
			t.Fatalf("max %d: %d keys, %v", maxDemand, len(keys), err)
		}
	}
	if keys, err := ranged(t, ctx, n, n, kvs.Ranger[int, int]{Snapshot: snap, Lo: 20000, Hi: 30000}, 10); err != nil || len(keys) != 0 {
		t.Fatalf("an empty range: %v %v", keys, err)
	}
}

// A Ranger stops once its client breaks off.
func TestRangerBreak(t *testing.T) {
	snap := ordered(t, 10000).Snapshot()
	n := proc.NewNode("")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pid, _ := stage.Start(ctx, n, stage.Produce(kvs.Ranger[int, int]{Snapshot: snap, Lo: 0, Hi: 10000}))
	watch, stop := n.Watch(ctx, pid)
	defer stop()
	for e, err := range stage.Pull[kvs.Entry[int, int]](ctx, n, stage.Subscribe{To: pid, Max: 10}) {
		if err != nil || e.Key == 50 {
			break
		}
	}
	<-watch.Done()
	if reason := context.Cause(watch); !errors.Is(reason, proc.Normal) {
		t.Fatalf("exited with %v", reason)
	}
}

func TestRangerOfSet(t *testing.T) {
	tab := open(t, t.TempDir(), kvs.Options[int, int]{})
	_, err := stage.Start(context.Background(), proc.NewNode(""), stage.Produce(kvs.Ranger[int, int]{Snapshot: tab.Snapshot()}))
	if !errors.Is(err, kvs.ErrRangerOfSet) {
		t.Fatalf("started with %v", err)
	}
}

// A client of another node goes over a range.
func TestRangerAcrossNodes(t *testing.T) {
	var mu sync.Mutex
	addrs := map[string]string{}
	resolve := func(node string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		a, ok := addrs[node]
		return a, ok
	}
	var nodes []*proc.Node
	for _, name := range []string{"a@ranger", "b@ranger"} {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "ranger", Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(d.Stop)
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		nodes = append(nodes, n)
	}
	snap := ordered(t, 5000).Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	keys, err := ranged(t, ctx, nodes[0], nodes[1], kvs.Ranger[int, int]{Snapshot: snap, Lo: 0, Hi: 5000}, 500)
	if err != nil || !slices.Equal(keys, span(0, 5000)) {
		t.Fatalf("%d keys, %v", len(keys), err)
	}
}
