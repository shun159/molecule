package stage_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// cluster starts distributed nodes of names, on the loopback.
func cluster(t testing.TB, names ...string) ([]*proc.Node, []*dist.Dist) {
	t.Helper()
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
	for _, name := range names {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "stage", Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(d.Stop)
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		nodes = append(nodes, n)
		dists = append(dists, d)
	}
	return nodes, dists
}

func init() { stage.Register[int]() }

// TestDist has a consumer of one node sum the events of a producer of
// another.
func TestDist(t *testing.T) {
	nodes, _ := cluster(t, "a@stage", "b@stage")
	a, b := nodes[0], nodes[1]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const limit = 10000
	src, err := stage.Start(ctx, a, stage.Produce(numbers{Limit: limit}))
	if err != nil {
		t.Fatal(err)
	}
	done := b.Alias()
	defer done.Release()
	if _, err := stage.Start(ctx, b, stage.Consume(summer{From: src, Max: 100, Done: actor.From{Tag: done.Ref}})); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-done.C:
		if sum := m.Msg.(int); sum != limit*(limit-1)/2 {
			t.Fatalf("sum %d", sum)
		}
	case <-ctx.Done():
		t.Fatal("no sum")
	}
}

// watchExit returns the reason pid of n exits with, within a while.
func watchExit(t *testing.T, n *proc.Node, pid proc.PID) error {
	t.Helper()
	ctx, stop := n.Watch(context.Background(), pid)
	defer stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-time.After(5 * time.Second):
		t.Fatalf("%v alive", pid)
		return nil
	}
}

// TestDistCancelled has a producer cancel a consumer of another node: the
// reason is that of stage, across nodes.
func TestDistCancelled(t *testing.T) {
	nodes, _ := cluster(t, "a@stage", "b@stage")
	a, b := nodes[0], nodes[1]
	ctx := context.Background()
	part := stage.Partition[int]{Partitions: 1, Hash: mod(1)}
	src, _ := stage.Start(ctx, a, stage.Produce(counter{Limit: 100, Dispatch: part}))
	if _, err := stage.Start(ctx, a, stage.Consume(sink{From: src})); err != nil {
		t.Fatal(err)
	}
	late, _ := stage.Start(ctx, b, stage.Consume(sink{From: src}))
	if reason := watchExit(t, b, late); !errors.Is(reason, stage.ErrPartitionTaken) {
		t.Fatalf("exited with %v", reason)
	}
}

// TestDistCancel has a consumer cancel with a reason of its own: the
// producer of the other node is free for the next, which gets the rest.
func TestDistCancel(t *testing.T) {
	nodes, _ := cluster(t, "a@stage", "b@stage")
	a, b := nodes[0], nodes[1]
	ctx := context.Background()
	part := stage.Partition[int]{Partitions: 1, Hash: mod(1)}
	src, _ := stage.Start(ctx, a, stage.Produce(counter{Limit: 100, Done: true, Dispatch: part}))
	first, _ := stage.Start(ctx, b, stage.Consume(sink{From: src, Max: 10}))
	b.Send(first, actor.Do(stage.Cancel{Tag: "src", Reason: errBoom}))

	for range 100 { // until the producer has the cancel
		next, _ := stage.Start(ctx, b, stage.Consume(sink{From: src, Max: 10}))
		reason := watchExit(t, b, next)
		if errors.Is(reason, proc.Normal) {
			return // took the partition, and the rest, the producer done
		}
		if !errors.Is(reason, stage.ErrPartitionTaken) {
			t.Fatalf("exited with %v", reason)
		}
	}
	t.Fatal("the partition never freed")
}

// TestDistNodeDown has the node of a producer lost: its consumer stops,
// the connection lost, and the producer is free of it.
func TestDistNodeDown(t *testing.T) {
	nodes, dists := cluster(t, "a@stage", "b@stage")
	a, b := nodes[0], nodes[1]
	ctx := context.Background()
	src, _ := stage.Start(ctx, a, stage.Produce(counter{Limit: 100}))
	dst, _ := stage.Start(ctx, b, stage.Consume(sink{From: src}))
	time.Sleep(50 * time.Millisecond)
	dists[1].Disconnect("a@stage")
	if reason := watchExit(t, b, dst); !errors.Is(reason, proc.NoConnection) {
		t.Fatalf("exited with %v", reason)
	}
}

// BenchmarkDistThroughput passes b.N integers from a producer of a node
// to a consumer of another, over the loopback: what an event costs
// across nodes.
func BenchmarkDistThroughput(b *testing.B) {
	for _, maxDemand := range []int{100, 1000, 10000} {
		b.Run("max="+strconv.Itoa(maxDemand), func(b *testing.B) {
			b.ReportAllocs()
			nodes, _ := cluster(b, "a@bench", "b@bench")
			ctx := context.Background()
			done := nodes[1].Alias()
			defer done.Release()
			src, err := stage.Start(ctx, nodes[0], stage.Produce(numbers{Limit: b.N}))
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			if _, err := stage.Start(ctx, nodes[1], stage.Consume(summer{From: src, Max: maxDemand, Done: actor.From{Tag: done.Ref}})); err != nil {
				b.Fatal(err)
			}
			if sum := (<-done.C).Msg.(int); sum != b.N*(b.N-1)/2 {
				b.Fatalf("sum %d", sum)
			}
		})
	}
}

// TestDistStats reads the Stats of a stage of another node: a producer
// with no consumer, its buffer full.
func TestDistStats(t *testing.T) {
	nodes, _ := cluster(t, "a@stage", "b@stage")
	a, b := nodes[0], nodes[1]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src, err := stage.Start(ctx, a, stage.Produce(counter{Buf: stage.Buffer{Size: 10}}), actor.WithName(actor.Local("src")))
	if err != nil {
		t.Fatal(err)
	}
	a.Send(src, actor.Do(stage.Emit[int]{Events: upTo(30)}))

	st, err := stage.StatsOf(ctx, b, actor.Remote{Node: "a@stage", Name: "src"})
	if err != nil {
		t.Fatal(err)
	}
	if want := (stage.Stats{Held: 10, Emitted: 30, Dropped: 20}); st != want {
		t.Fatalf("%+v, want %+v", st, want)
	}
}
