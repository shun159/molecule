package flow_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/flow"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

func sum(a, x int) int { return a + x }
func zero() int        { return 0 }

// Each worker reduces its events, and lets its sum out at the end; reduced
// again, after a Partition of one, they are one.
func TestReduce(t *testing.T) {
	n := proc.NewNode("")
	src := source(t, n, numbers{Limit: 1000})
	partial, err := collect(t, flow.Run(timeout(t), n, flow.Reduce(flow.From[int](src), zero, sum), flow.Workers(4), flow.Max(16)))
	if err != nil || len(partial) != 4 || sumOf(partial) != 999*1000/2 {
		t.Fatalf("%v, %v", partial, err)
	}

	src = source(t, n, numbers{Limit: 1000})
	one := flow.Reduce(flow.Partition(flow.Reduce(flow.From[int](src), zero, sum), 1, func(int) int { return 0 }), zero, sum)
	total, err := collect(t, flow.Run(timeout(t), n, flow.Map(one, func(x int) int { return -x }), flow.Workers(4)))
	if err != nil || !slices.Equal(total, []int{-999 * 1000 / 2}) {
		t.Fatalf("%v, %v", total, err)
	}
}

func sumOf(xs []int) int {
	s := 0
	for _, x := range xs {
		s += x
	}
	return s
}

// Partitioned by key, the events of a key are all reduced by one worker:
// each key comes out once. Not partitioned, a key comes out of each
// worker that had some of it.
func TestReduceByKey(t *testing.T) {
	n := proc.NewNode("")
	count := func(c, _ int) int { return c + 1 }
	src := source(t, n, numbers{Limit: 10000})
	byKey := flow.ReduceByKey(flow.Partition(flow.From[int](src), 4, mod(10)), mod(10), zero, count)
	kvs, err := collect(t, flow.Run(timeout(t), n, byKey, flow.Workers(3), flow.Max(64)))
	if err != nil || len(kvs) != 10 {
		t.Fatalf("%d keys, %v", len(kvs), err)
	}
	for _, kv := range kvs {
		if kv.Value != 1000 {
			t.Fatalf("key %d: %d", kv.Key, kv.Value)
		}
	}

	src = source(t, n, numbers{Limit: 10000})
	split, err := collect(t, flow.Run(timeout(t), n, flow.ReduceByKey(flow.From[int](src), mod(10), zero, count), flow.Workers(3), flow.Max(64)))
	total := 0
	for _, kv := range split {
		total += kv.Value
	}
	if err != nil || total != 10000 || len(split) < 10 {
		t.Fatalf("%d keys, %d events, %v", len(split), total, err)
	}
}

func TestReduceTwice(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a layer reduced twice")
		}
	}()
	flow.Reduce(flow.Reduce(flow.From[int](), zero, sum), zero, sum)
}

// Reduced every window, the events come out of windows aligned on the
// window, all of them, the last at the end.
func TestReduceEvery(t *testing.T) {
	n := proc.NewNode("")
	const every = 20 * time.Millisecond
	src := source(t, n, numbers{Limit: 5000, Open: true})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	windows := 0
	counted := 0
	for w, err := range flow.Run(ctx, n, flow.ReduceEvery(flow.From[int](src), every, zero, func(c, _ int) int { return c + 1 }), flow.Workers(2)) {
		if err != nil {
			t.Fatal(err)
		}
		if !w.Start.Equal(w.Start.Truncate(every)) {
			t.Fatalf("a window of %v", w.Start)
		}
		windows++
		if counted += w.Acc; counted == 5000 && windows >= 4 {
			break // the source is open: it never ends
		}
	}
	if counted != 5000 {
		t.Fatalf("%d events in %d windows", counted, windows)
	}
}

// A flow counts the values of a range of a table, by value.
func TestReduceRange(t *testing.T) {
	tab, err := kvs.Open(t.TempDir(), kvs.Options[int, int]{Type: kvs.OrderedSet, Sync: kvs.SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	for k := range 7000 {
		tab.Put(k, k%7)
	}
	n := proc.NewNode("")
	r, err := stage.Start(context.Background(), n, stage.Produce(kvs.Ranger[int, int]{Snapshot: tab.Snapshot(), Lo: 0, Hi: 7000}))
	if err != nil {
		t.Fatal(err)
	}
	value := func(e kvs.Entry[int, int]) int { return e.Value }
	byValue := flow.ReduceByKey(flow.Partition(flow.From[kvs.Entry[int, int]](r), 3, value), value, zero,
		func(c int, _ kvs.Entry[int, int]) int { return c + 1 })
	counts, err := collect(t, flow.Run(timeout(t), n, byValue, flow.Workers(4)))
	if err != nil || len(counts) != 7 {
		t.Fatalf("%v, %v", counts, err)
	}
	for _, kv := range counts {
		if kv.Value != 1000 {
			t.Fatalf("value %d: %d", kv.Key, kv.Value)
		}
	}
}
