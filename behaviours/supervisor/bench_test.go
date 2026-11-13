package supervisor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

// BenchmarkStartChild starts a child of a dynamic supervisor with many
// already: a pure state of them all is not copied whole on each.
func BenchmarkStartChild(b *testing.B) {
	idle := supervisor.ChildSpec{ID: "idle", Restart: supervisor.Temporary, Start: supervisor.StartFunc(
		func(_ context.Context, parent *proc.Self) (proc.PID, error) {
			return parent.SpawnLink(func(s *proc.Self) error {
				_, err := s.Receive(context.Background())
				return err
			}), nil
		})}
	for _, n := range []int{0, 1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			node := proc.NewNode("")
			ctx := context.Background()
			sup, err := supervisor.StartDynamic(ctx, node, supervisor.DynamicSpec{})
			if err != nil {
				b.Fatal(err)
			}
			for range n {
				if _, err := supervisor.StartChild(ctx, node, sup, idle); err != nil {
					b.Fatal(err)
				}
			}
			for b.Loop() {
				if _, err := supervisor.StartChild(ctx, node, sup, idle); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
