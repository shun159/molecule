package flow_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/flow"
	"github.com/shun159/molecule/kvs"
	"github.com/shun159/molecule/proc"
)

// numbers emits the integers below Limit, as asked, then is done unless
// Open; with Alone, it stops once its consumers are gone.
type numbers struct {
	Limit int
	Open  bool
	Alone bool
}

func (numbers) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (q numbers) HandleDemand(next, n int) (int, []actor.Effect) {
	k := min(n, q.Limit-next)
	events := make([]int, k)
	for i := range events {
		events[i] = next + i
	}
	out := actor.Do(stage.Emit[int]{Events: events})
	if next+k == q.Limit && !q.Open {
		out = append(out, stage.Done{})
	}
	return next + k, out
}

// HandleInfo returns the effects it is sent.
func (numbers) HandleInfo(next int, msg any) (int, []actor.Effect) {
	effs, _ := msg.([]actor.Effect)
	return next, effs
}

func (q numbers) StopAlone() bool { return q.Alone }

func source(t *testing.T, n *proc.Node, q numbers) proc.PID {
	t.Helper()
	pid, err := stage.Start(context.Background(), n, stage.Produce(q))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

func collect[T any](t *testing.T, seq func(func(T, error) bool)) ([]T, error) {
	t.Helper()
	var got []T
	for v, err := range seq {
		if err != nil {
			return got, err
		}
		got = append(got, v)
	}
	return got, nil
}

func timeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// The operations of a flow are fused, and run by its workers on all the
// events of its source.
func TestRun(t *testing.T) {
	n := proc.NewNode("")
	src := source(t, n, numbers{Limit: 1000})
	f := flow.From[int](src)
	doubled := flow.Map(f, func(x int) int { return 2 * x })
	thirds := flow.Filter(doubled, func(x int) bool { return x%3 == 0 })
	pairs := flow.FlatMap(thirds, func(x int, yield func(int)) { yield(x); yield(x + 1) })

	got, err := collect(t, flow.Run(timeout(t), n, pairs, flow.Workers(4), flow.Max(64)))
	if err != nil {
		t.Fatal(err)
	}
	var want []int
	for x := range 1000 {
		if 2*x%3 == 0 {
			want = append(want, 2*x, 2*x+1)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("%d events, want %d", len(got), len(want))
	}
}

// One worker keeps the order of the source.
func TestRunInOrder(t *testing.T) {
	n := proc.NewNode("")
	src := source(t, n, numbers{Limit: 500})
	got, err := collect(t, flow.Run(timeout(t), n, flow.Map(flow.From[int](src), func(x int) int { return -x }), flow.Workers(1), flow.Max(7)))
	if err != nil || len(got) != 500 || !slices.IsSortedFunc(got, func(a, b int) int { return b - a }) {
		t.Fatalf("%d events, sorted %v, %v", len(got), slices.IsSorted(got), err)
	}
}

// A flow of several sources ends once all are done.
func TestRunSources(t *testing.T) {
	n := proc.NewNode("")
	a, b := source(t, n, numbers{Limit: 300}), source(t, n, numbers{Limit: 50})
	got, err := collect(t, flow.Run(timeout(t), n, flow.From[int](a, b), flow.Workers(3), flow.Max(16)))
	if err != nil || len(got) != 350 {
		t.Fatalf("%d events, %v", len(got), err)
	}
}

// Broken off, a flow stops its workers, and an Ephemeral source; a source
// failing, the range ends with its reason.
func TestRunEnds(t *testing.T) {
	n := proc.NewNode("")
	ctx := timeout(t)
	src := source(t, n, numbers{Limit: 1 << 30, Alone: true})
	watch, stop := n.Watch(ctx, src)
	defer stop()
	for _, err := range flow.Run(ctx, n, flow.From[int](src), flow.Workers(2), flow.Max(10)) {
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
	if _, err := collect(t, flow.Run(ctx, n, flow.From[int](bad), flow.Workers(2))); !errors.Is(err, boom) {
		t.Fatalf("ended with %v", err)
	}
}

// A flow over a range of a table: the entries of a snapshot, mapped and
// filtered on all the cores.
func TestRunRange(t *testing.T) {
	tab, err := kvs.Open(t.TempDir(), kvs.Options[int, int]{Type: kvs.OrderedSet, Sync: kvs.SyncNone})
	if err != nil {
		t.Fatal(err)
	}
	defer tab.Close()
	for k := range 10000 {
		tab.Put(k, k%7)
	}
	n := proc.NewNode("")
	ranger, err := stage.Start(context.Background(), n, stage.Produce(kvs.Ranger[int, int]{Snapshot: tab.Snapshot(), Lo: 0, Hi: 10000}))
	if err != nil {
		t.Fatal(err)
	}
	entries := flow.From[kvs.Entry[int, int]](ranger)
	sevens := flow.Filter(entries, func(e kvs.Entry[int, int]) bool { return e.Value == 0 })
	keys := flow.Map(sevens, func(e kvs.Entry[int, int]) int { return e.Key })
	sum := 0
	for k, err := range flow.Run(timeout(t), n, keys, flow.Workers(4)) {
		if err != nil {
			t.Fatal(err)
		}
		sum += k
	}
	want := 0
	for k := 0; k < 10000; k += 7 {
		want += k
	}
	if sum != want {
		t.Fatalf("sum %d, want %d", sum, want)
	}
}

// A source of fewer events than workers is done before all could have
// subscribed, but for their being held until all have.
func TestRunFewEvents(t *testing.T) {
	n := proc.NewNode("")
	for range 20 {
		src := source(t, n, numbers{Limit: 1})
		got, err := collect(t, flow.Run(timeout(t), n, flow.From[int](src), flow.Workers(8), flow.Max(1)))
		if err != nil || len(got) != 1 {
			t.Fatalf("%v, %v", got, err)
		}
	}
}
