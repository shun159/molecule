package genserver_test

import (
	"context"
	"fmt"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// Counter counts. Its requests form a closed set, like an Erlang tagged
// tuple: each is a type implementing CounterReq.
type Counter struct {
	Initial int
}

type CounterReq interface{ counterReq() }

type (
	Get   struct{}
	Reset struct{}
)

func (Get) counterReq()   {}
func (Reset) counterReq() {}

// Add is the only cast.
type Add struct{ N int }

func (c Counter) Init(proc.PID) (int, []molecule.Effect, error) { return c.Initial, nil, nil }

func (Counter) HandleCall(n int, req CounterReq, from genserver.From[int]) (int, []molecule.Effect) {
	switch req.(type) {
	case Get:
		return n, molecule.Do(from.Reply(n))
	case Reset:
		return 0, molecule.Do(from.Reply(n))
	}
	return n, nil
}

func (Counter) HandleCast(n int, msg Add) (int, []molecule.Effect) {
	return n + msg.N, nil
}

func Example() {
	n := proc.NewNode("")
	ctx := context.Background()

	counter, err := genserver.Start(ctx, n, Counter{Initial: 10}, molecule.WithName(molecule.Local("counter")))
	if err != nil {
		panic(err)
	}
	counter.Cast(n, Add{5})
	v, _ := counter.Call(ctx, n, Get{})
	fmt.Println("get:", v)

	// Elsewhere, reach it by name with the types taken from the behaviour.
	byName := genserver.RefFor(Counter{}, molecule.Local("counter"))
	v, _ = byName.Call(ctx, n, Reset{})
	fmt.Println("reset returned:", v)
	v, _ = byName.Call(ctx, n, Get{})
	fmt.Println("get:", v)

	// Output:
	// get: 15
	// reset returned: 15
	// get: 0
}

// Barrier holds callers until N of them are waiting, then lets them all
// go. It shows a deferred reply: each From is kept in the state and
// replied to later.
type Barrier struct {
	genserver.Default[[]genserver.From[string]] // the callers waiting
	N                                           int
}

type Wait struct{}

func (b Barrier) HandleCall(waiting []genserver.From[string], _ Wait, from genserver.From[string]) ([]genserver.From[string], []molecule.Effect) {
	waiting = append(waiting[:len(waiting):len(waiting)], from)
	if len(waiting) < b.N {
		return waiting, nil // no reply yet
	}
	var effs []molecule.Effect
	for _, w := range waiting {
		effs = append(effs, w.Reply("go"))
	}
	return nil, effs
}

func Example_deferredReply() {
	n := proc.NewNode("")
	ctx := context.Background()
	barrier, _ := genserver.Start(ctx, n, Barrier{N: 3})

	done := make(chan string)
	for range 3 {
		go func() {
			v, _ := barrier.Call(ctx, n, Wait{})
			done <- v
		}()
	}
	fmt.Println(<-done, <-done, <-done)

	// Output:
	// go go go
}
