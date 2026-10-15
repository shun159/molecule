package genserver_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
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

// asker has requests out to a counter, depth at once, until it has the
// answers of n, as a behaviour asks: SendRequest, the answer a message.
type asker struct {
	genserver.Default[asking]
	counter genserver.Ref[CounterReq, int, Add]
}

type (
	asking struct {
		left, out int
		done      chan struct{}
	}
	ask struct {
		n, depth int
		done     chan struct{}
	}
)

func (a asker) HandleCast(_ asking, m ask) (asking, []actor.Effect) {
	s := asking{left: m.n, done: m.done}
	var effs []actor.Effect
	for range min(m.depth, m.n) {
		s.left, s.out = s.left-1, s.out+1
		effs = append(effs, a.counter.CallEffect(Get{}, nil))
	}
	return s, effs
}

func (a asker) HandleInfo(s asking, msg any) (asking, []actor.Effect) {
	if _, ok := msg.(actor.Response); !ok {
		return s, nil
	}
	s.out--
	if s.left > 0 {
		s.left, s.out = s.left-1, s.out+1
		return s, actor.Do(a.counter.CallEffect(Get{}, nil))
	}
	if s.out == 0 {
		close(s.done)
	}
	return s, nil
}

func BenchmarkSendRequest(b *testing.B) {
	for _, depth := range []int{1, 64} {
		b.Run(fmt.Sprintf("out-%d", depth), func(b *testing.B) {
			n, c := startCounter(b)
			a, err := genserver.Start(context.Background(), n, asker{counter: c})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			done := make(chan struct{})
			a.Cast(n, ask{n: b.N, depth: depth, done: done})
			<-done
		})
	}
}
