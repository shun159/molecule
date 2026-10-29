package stage_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

func pulled(t *testing.T, ctx context.Context, n *proc.Node, sub stage.Subscribe) ([]int, error) {
	t.Helper()
	var got []int
	for v, err := range stage.Pull[int](ctx, n, sub) {
		if err != nil {
			return got, err
		}
		got = append(got, v)
	}
	return got, nil
}

// TestPull ranges over the events of a producer, to its end.
func TestPull(t *testing.T) {
	n := proc.NewNode("")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src, _ := stage.Start(ctx, n, stage.Produce(counter{Limit: 1000, Over: 3, Done: true}))
	got, err := pulled(t, ctx, n, stage.Subscribe{To: src, Max: 64})
	if err != nil || !slices.Equal(got, upTo(1000)) {
		t.Fatalf("%d events, %v", len(got), err)
	}
}

// TestPullBreak breaks off a range: the producer, Ephemeral, stops, having
// emitted no more than Max ahead.
func TestPullBreak(t *testing.T) {
	n := proc.NewNode("")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src, _ := stage.Start(ctx, n, stage.Produce(counter{Limit: 1 << 30, Alone: true}))
	watch, stop := n.Watch(ctx, src)
	defer stop()
	taken := 0
	for v, err := range stage.Pull[int](ctx, n, stage.Subscribe{To: src, Max: 10}) {
		if err != nil {
			t.Fatal(err)
		}
		if taken++; taken == 25 {
			st, err := stage.StatsOf(ctx, n, src)
			if err != nil || st.Emitted > 25+10 {
				t.Fatalf("%d emitted, %d taken: %v", st.Emitted, taken, err)
			}
			_ = v
			break
		}
	}
	<-watch.Done()
	if reason := context.Cause(watch); !errors.Is(reason, proc.Normal) {
		t.Fatalf("the producer exited with %v", reason)
	}
}

// TestPullEnds has a subscription end other than normally: the range ends
// with its reason; or no producer at all.
func TestPullEnds(t *testing.T) {
	n := proc.NewNode("")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src, _ := stage.Start(ctx, n, stage.Produce(counter{Limit: 5}))
	go func() {
		time.Sleep(50 * time.Millisecond)
		n.Send(src, actor.Do(actor.Stop{Reason: errBoom}))
	}()
	got, err := pulled(t, ctx, n, stage.Subscribe{To: src})
	if !errors.Is(err, errBoom) || !slices.Equal(got, upTo(5)) {
		t.Fatalf("%v, %v", got, err)
	}
	if _, err := pulled(t, ctx, n, stage.Subscribe{To: actor.Local("none")}); !errors.Is(err, proc.NoProc) {
		t.Fatalf("of no producer: %v", err)
	}
}

// TestPullContext has the context done while the range waits.
func TestPullContext(t *testing.T) {
	n := proc.NewNode("")
	src, _ := stage.Start(context.Background(), n, stage.Produce(counter{})) // never emits
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := pulled(t, ctx, n, stage.Subscribe{To: src}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v", err)
	}
}

// TestDistPull ranges over the events of a producer of another node.
func TestDistPull(t *testing.T) {
	nodes, _ := cluster(t, "a@pull", "b@pull")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	src, _ := stage.Start(ctx, nodes[0], stage.Produce(counter{Limit: 5000, Done: true}))
	got, err := pulled(t, ctx, nodes[1], stage.Subscribe{To: src, Max: 500})
	if err != nil || !slices.Equal(got, upTo(5000)) {
		t.Fatalf("%d events, %v", len(got), err)
	}
}
