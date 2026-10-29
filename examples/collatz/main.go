// Command collatz finds the number below N with the longest Collatz
// chain, by a pipeline of stages working on all the cores: work that is
// all CPU, cut in pieces.
//
//	collatz_ranges  producer: the ranges of numbers, as the workers ask
//	collatz_worker  producer-consumers: the longest chain of each range
//	collatz_best    consumer: the longest of all
//
// The ranges go each to one worker, the one wanting most, and a worker
// asks for another as it is done with one: the fast take more. The
// ranges done, each stage ends once it has passed on all it had.
//
//	go run ./examples/collatz -n 50000000 -workers 8 -compare
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/shun159/molecule/proc"
)

func main() {
	n := flag.Int("n", 10_000_000, "look below n")
	chunk := flag.Int("chunk", 20_000, "numbers in a range, the piece of work")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "workers")
	compare := flag.Bool("compare", false, "time a loop on one core as well")
	flag.Parse()

	node := proc.NewNode("")
	start := time.Now()
	best, err := Run(context.Background(), node, *n, *chunk, *workers)
	if err != nil {
		log.Fatal(err)
	}
	took := time.Since(start)
	fmt.Printf("%d has the longest chain below %d, of %d steps\n", best.Start, *n, best.Steps)
	fmt.Printf("%d workers: %v\n", *workers, took.Round(time.Millisecond))

	if *compare {
		start = time.Now()
		same := Longest(span{Lo: 1, Hi: *n}) == best
		one := time.Since(start)
		fmt.Printf("one core:  %v, %.1fx as long, the same: %v\n", one.Round(time.Millisecond), float64(one)/float64(took), same)
	}
}
