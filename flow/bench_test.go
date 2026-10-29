package flow_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/shun159/molecule/flow"
	"github.com/shun159/molecule/proc"
)

// BenchmarkRun runs a flow of three operations over b.N integers: what an
// event costs a flow, the work of none, on one worker and on several.
func BenchmarkRun(b *testing.B) {
	for _, workers := range []int{1, 4} {
		b.Run("workers="+strconv.Itoa(workers), func(b *testing.B) {
			b.ReportAllocs()
			n := proc.NewNode("")
			src := source(&testing.T{}, n, numbers{Limit: b.N})
			f := flow.Map(flow.Filter(flow.Map(flow.From[int](src), func(x int) int { return x + 1 }),
				func(x int) bool { return x > 0 }), func(x int) int { return x - 1 })
			b.ResetTimer()
			sum := 0
			for x, err := range flow.Run(context.Background(), n, f, flow.Workers(workers)) {
				if err != nil {
					b.Fatal(err)
				}
				sum += x
			}
			if sum != b.N*(b.N-1)/2 {
				b.Fatalf("sum %d", sum)
			}
		})
	}
}

// BenchmarkReduceByKey counts b.N integers by key, of 10 keys and of 1000,
// partitioned among 4 workers: what an event costs a keyed reduce.
func BenchmarkReduceByKey(b *testing.B) {
	for _, keys := range []int{10, 1000} {
		b.Run(strconv.Itoa(keys), func(b *testing.B) { reduceByKey(b, keys) })
	}
}

func reduceByKey(b *testing.B, keys int) {
	b.ReportAllocs()
	n := proc.NewNode("")
	src := source(&testing.T{}, n, numbers{Limit: b.N})
	key := func(x int) int { return x % keys }
	f := flow.ReduceByKey(flow.Partition(flow.From[int](src), 4, key), key,
		func() int { return 0 }, func(c, _ int) int { return c + 1 })
	b.ResetTimer()
	total := 0
	for kv, err := range flow.Run(context.Background(), n, f, flow.Workers(4)) {
		if err != nil {
			b.Fatal(err)
		}
		total += kv.Value
	}
	if total != b.N {
		b.Fatalf("%d counted", total)
	}
}
