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
