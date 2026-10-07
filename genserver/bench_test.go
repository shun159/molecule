package genserver_test

import (
	"context"
	"sync"
	"testing"

	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// Baselines: a counter served by a goroutine over channels, and one
// guarded by a mutex.

// chanCounter keeps adds and gets in one FIFO channel, so that a get sees
// every add sent before it, as with a mailbox.
type chanCounter struct {
	ops chan chanOp
}

type chanOp struct {
	add   int
	reply chan int // nil for an add
}

func newChanCounter() *chanCounter {
	c := &chanCounter{ops: make(chan chanOp, 1024)}
	go func() {
		n := 0
		for op := range c.ops {
			if op.reply != nil {
				op.reply <- n
			} else {
				n += op.add
			}
		}
	}()
	return c
}

func (c *chanCounter) Get() int {
	r := make(chan int, 1)
	c.ops <- chanOp{reply: r}
	return <-r
}

func (c *chanCounter) Add(d int) { c.ops <- chanOp{add: d} }

type mutexCounter struct {
	mu sync.Mutex
	n  int
}

func (c *mutexCounter) Get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func startCounter(b *testing.B) (*proc.Node, genserver.Ref[CounterReq, int, Add]) {
	n := proc.NewNode("")
	c, err := genserver.Start(context.Background(), n, Counter{})
	if err != nil {
		b.Fatal(err)
	}
	return n, c
}

func BenchmarkCall(b *testing.B) {
	ctx := context.Background()
	b.Run("genserver", func(b *testing.B) {
		n, c := startCounter(b)
		for b.Loop() {
			if _, err := c.Call(ctx, n, Get{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("baseline-chan", func(b *testing.B) {
		c := newChanCounter()
		for b.Loop() {
			c.Get()
		}
	})
	b.Run("baseline-mutex", func(b *testing.B) {
		var c mutexCounter
		for b.Loop() {
			c.Get()
		}
	})
}

func BenchmarkCallParallel(b *testing.B) {
	ctx := context.Background()
	b.Run("genserver", func(b *testing.B) {
		n, c := startCounter(b)
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := c.Call(ctx, n, Get{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
	b.Run("baseline-chan", func(b *testing.B) {
		c := newChanCounter()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Get()
			}
		})
	})
	b.Run("baseline-mutex", func(b *testing.B) {
		var c mutexCounter
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				c.Get()
			}
		})
	})
}

func BenchmarkCast(b *testing.B) {
	ctx := context.Background()
	b.Run("genserver", func(b *testing.B) {
		n, c := startCounter(b)
		for b.Loop() {
			c.Cast(n, Add{1})
		}
		if v, _ := c.Call(ctx, n, Get{}); v != b.N {
			b.Fatalf("count = %d, want %d", v, b.N)
		}
	})
	b.Run("baseline-chan", func(b *testing.B) {
		c := newChanCounter()
		for b.Loop() {
			c.Add(1)
		}
		if v := c.Get(); v != b.N {
			b.Fatalf("count = %d, want %d", v, b.N)
		}
	})
}
