package dist_test

import (
	"context"
	"testing"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// flush asks a sink to tell from once it has the messages sent before.
type flush struct{ From proc.PID }

func init() { dist.Register(flush{}) }

// BenchmarkConnection sends pings from a node to another over loopback:
// one at a time, each echoed back, and in a stream, gathered into writes.
func BenchmarkConnection(b *testing.B) {
	nodes, _ := cluster(b, "secret", "a@test", "b@test")
	na, nb := nodes[0], nodes[1]
	pa, ca := inbox(na)
	b.Run("pingpong", func(b *testing.B) {
		echo := nb.Spawn(func(s *proc.Self) error {
			for {
				msg, err := s.Receive(context.Background())
				if err != nil {
					return err
				}
				p := msg.(Ping)
				s.Send(p.From, p)
			}
		})
		for i := 0; b.Loop(); i++ {
			na.Send(echo, Ping{pa, i})
			<-ca
		}
	})
	b.Run("stream", func(b *testing.B) {
		sink := nb.Spawn(func(s *proc.Self) error {
			for {
				msg, err := s.Receive(context.Background())
				if err != nil {
					return err
				}
				if f, ok := msg.(flush); ok {
					s.Send(f.From, f)
				}
			}
		})
		// A window of pings at a time, so that the mailboxes stay small.
		const window = 1024
		for i := 0; b.Loop(); i++ {
			na.Send(sink, Ping{pa, i})
			if i%window == window-1 {
				na.Send(sink, flush{pa})
				<-ca
			}
		}
	})
}
