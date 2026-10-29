package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/shun159/molecule/proc"
)

// The work of a run of the benchmarks: all CPU, about 0.2s on one core.
const benchN = 1 << 20

// BenchmarkLongest finds the longest chain below benchN on one core, with
// plain goroutines taking ranges from a channel, and with the pipeline of
// stages, for as many workers as given, the work cut in ranges of chunk
// numbers: what the stages cost against the goroutines, as the work
// spreads on the cores, and as the pieces get small. A range of 20000
// numbers is about 4ms of work, one of 100, 20µs.
func BenchmarkLongest(b *testing.B) {
	b.Run("loop", func(b *testing.B) {
		for b.Loop() {
			Longest(span{Lo: 1, Hi: benchN})
		}
	})
	for _, chunk := range []int{20_000, 1000, 100} {
		for _, workers := range benchWorkers() {
			b.Run(fmt.Sprintf("chunk=%d/goroutines/%d", chunk, workers), func(b *testing.B) {
				for b.Loop() {
					goroutines(benchN, chunk, workers)
				}
			})
			b.Run(fmt.Sprintf("chunk=%d/stages/%d", chunk, workers), func(b *testing.B) {
				node := proc.NewNode("")
				for b.Loop() {
					if _, err := Run(context.Background(), node, benchN, chunk, workers); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// benchWorkers is 1, 2, 4... up to the cores, and twice as many.
func benchWorkers() []int {
	var ws []int
	for w := 1; w <= 2*runtime.GOMAXPROCS(0); w *= 2 {
		ws = append(ws, w)
	}
	return ws
}

// goroutines is Run with plain goroutines: workers take ranges from a
// channel, and send their longest to another.
func goroutines(n, chunk, workers int) chain {
	spans := make(chan span)
	chains := make(chan chain, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for s := range spans {
				chains <- Longest(s)
			}
		})
	}
	go func() {
		for lo := 1; lo < n; lo += chunk {
			spans <- span{Lo: lo, Hi: min(lo+chunk, n)}
		}
		close(spans)
		wg.Wait()
		close(chains)
	}()
	best := chain{Steps: -1}
	for c := range chains {
		if c.Steps > best.Steps || c.Steps == best.Steps && c.Start < best.Start {
			best = c
		}
	}
	return best
}
