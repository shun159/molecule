package stage_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/proc"
)

// numbers emits the integers below Limit, as asked, and is then done.
type numbers struct{ Limit int }

func (numbers) Init(proc.PID) (int, []actor.Effect, error) { return 0, nil, nil }

func (q numbers) HandleDemand(next, n int) (int, []actor.Effect) {
	k := min(n, q.Limit-next)
	events := make([]int, k)
	for i := range events {
		events[i] = next + i
	}
	out := actor.Do(stage.Emit[int]{Events: events})
	if next+k == q.Limit {
		out = append(out, stage.Done{})
	}
	return next + k, out
}

// summer sums the events of its producer, and tells Done the sum once
// its producer is done.
type summer struct {
	From actor.Dest
	Max  int
	Done actor.From
}

func (s summer) Init(proc.PID) (int, []actor.Effect, error) {
	return 0, actor.Do(stage.Subscribe{To: s.From, Max: s.Max}), nil
}

func (summer) HandleEvents(sum int, events []int, _ any) (int, []actor.Effect) {
	for _, n := range events {
		sum += n
	}
	return sum, nil
}

func (s summer) Terminate(sum int, _ error) []actor.Effect {
	return actor.Do(actor.Reply{To: s.Done, Value: sum})
}

// BenchmarkThroughput passes b.N integers from a producer to a consumer,
// which sums them: what a stage costs an event, the work of none. Baseline:
// a goroutine sending them to another over a channel, one by one, and in
// slices of as many as a batch of the stages.
func BenchmarkThroughput(b *testing.B) {
	for _, maxDemand := range []int{10, 100, 1000} {
		b.Run("stages/max="+strconv.Itoa(maxDemand), func(b *testing.B) {
			b.ReportAllocs()
			n := proc.NewNode("")
			ctx := context.Background()
			done := n.Alias()
			defer done.Release()
			src, err := stage.Start(ctx, n, stage.Produce(numbers{Limit: b.N}))
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			if _, err := stage.Start(ctx, n, stage.Consume(summer{From: src, Max: maxDemand, Done: actor.From{Tag: done.Ref}})); err != nil {
				b.Fatal(err)
			}
			if sum := (<-done.C).Msg.(int); sum != b.N*(b.N-1)/2 {
				b.Fatalf("sum %d", sum)
			}
		})
	}
	b.Run("chan", func(b *testing.B) {
		b.ReportAllocs()
		ch := make(chan int, 1000)
		go func() {
			for i := range b.N {
				ch <- i
			}
			close(ch)
		}()
		sum := 0
		for n := range ch {
			sum += n
		}
	})
	b.Run("chan/slices", func(b *testing.B) {
		b.ReportAllocs()
		const batch = 250 // Max-Min of a Max of 1000
		ch := make(chan []int, 4)
		go func() {
			for lo := 0; lo < b.N; lo += batch {
				events := make([]int, min(batch, b.N-lo))
				for i := range events {
					events[i] = lo + i
				}
				ch <- events
			}
			close(ch)
		}()
		sum := 0
		for events := range ch {
			for _, n := range events {
				sum += n
			}
		}
	})
}
