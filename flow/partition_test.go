package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/flow"
	"github.com/shun159/molecule/proc"
)

func mod(k int) func(int) int { return func(x int) int { return x % k } }

// The events of a flow partitioned all go through the layer after.
func TestPartition(t *testing.T) {
	n := proc.NewNode("")
	src := source(t, n, numbers{Limit: 1000})
	f := flow.Map(flow.Partition(flow.From[int](src), 4, mod(10)), func(x int) int { return 2 * x })
	got, err := collect(t, flow.Run(timeout(t), n, f, flow.Workers(3), flow.Max(32)))
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(got)
	want := make([]int, 1000)
	for i := range want {
		want[i] = 2 * i
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%d events", len(got))
	}
}

// Partitioned twice, a flow runs in three layers.
func TestPartitionTwice(t *testing.T) {
	n := proc.NewNode("")
	src := source(t, n, numbers{Limit: 2000})
	f := flow.Partition(flow.Partition(flow.From[int](src), 3, mod(7)), 5, mod(11))
	got, err := collect(t, flow.Run(timeout(t), n, f, flow.Workers(2), flow.Max(50)))
	if err != nil || len(got) != 2000 {
		t.Fatalf("%d events, %v", len(got), err)
	}
}

// From one worker, the events of a key go through their partition in
// order.
func TestPartitionKeyOrder(t *testing.T) {
	n := proc.NewNode("")
	src := source(t, n, numbers{Limit: 3000})
	got, err := collect(t, flow.Run(timeout(t), n, flow.Partition(flow.From[int](src), 3, mod(5)), flow.Workers(1), flow.Max(16)))
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[int][]int{}
	for _, x := range got {
		byKey[x%5] = append(byKey[x%5], x)
	}
	for k, xs := range byKey {
		if !slices.IsSorted(xs) || len(xs) != 600 {
			t.Fatalf("key %d: %d events, sorted %v", k, len(xs), slices.IsSorted(xs))
		}
	}
}

// A source of fewer events than workers, partitioned: no worker comes late
// to another done.
func TestPartitionFewEvents(t *testing.T) {
	n := proc.NewNode("")
	for range 20 {
		src := source(t, n, numbers{Limit: 1})
		got, err := collect(t, flow.Run(timeout(t), n, flow.Partition(flow.From[int](src), 4, mod(3)), flow.Workers(8), flow.Max(1)))
		if err != nil || len(got) != 1 {
			t.Fatalf("%v, %v", got, err)
		}
	}
}

// Broken off, a partitioned flow stops all its workers, and an Ephemeral
// source; a source failing, the range ends with its reason, through the
// layers.
func TestPartitionEnds(t *testing.T) {
	n := proc.NewNode("")
	ctx := timeout(t)
	src := source(t, n, numbers{Limit: 1 << 30, Alone: true})
	watch, stop := n.Watch(ctx, src)
	defer stop()
	for _, err := range flow.Run(ctx, n, flow.Partition(flow.From[int](src), 3, mod(3)), flow.Workers(2), flow.Max(10)) {
		if err != nil {
			t.Fatal(err)
		}
		break
	}
	<-watch.Done()
	if reason := context.Cause(watch); !errors.Is(reason, proc.Normal) {
		t.Fatalf("the source exited with %v", reason)
	}

	boom := errors.New("boom")
	bad := source(t, n, numbers{Limit: 10, Open: true})
	go func() {
		time.Sleep(50 * time.Millisecond)
		n.Send(bad, actor.Do(actor.Stop{Reason: boom}))
	}()
	if _, err := collect(t, flow.Run(ctx, n, flow.Partition(flow.From[int](bad), 2, mod(2)), flow.Workers(2))); !errors.Is(err, boom) {
		t.Fatalf("ended with %v", err)
	}
}
