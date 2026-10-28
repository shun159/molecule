package main

import (
	"context"
	"testing"

	"github.com/shun159/molecule/proc"
)

// The longest chain below a million is the one known.
func TestKnown(t *testing.T) {
	got, err := Run(context.Background(), proc.NewNode(""), 1_000_000, 10_000, 4)
	if err != nil {
		t.Fatal(err)
	}
	if want := (chain{Start: 837799, Steps: 524}); got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

// The pipeline finds what the loop finds, however the work is cut.
func TestRunAsLoop(t *testing.T) {
	const n = 50_000
	want := Longest(span{Lo: 1, Hi: n})
	for _, chunk := range []int{1, 7, 1000, n} {
		for _, workers := range []int{1, 3, 8} {
			got, err := Run(context.Background(), proc.NewNode(""), n, chunk, workers)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("chunk %d, %d workers: got %+v, want %+v", chunk, workers, got, want)
			}
		}
	}
}
