package proc

import (
	"context"
	"sync"
	"testing"
)

// The baselines do the same with plain goroutines and channels, to show
// what the process abstraction costs.

func BenchmarkPingPong(b *testing.B) {
	b.Run("proc", func(b *testing.B) {
		n := NewNode("")
		type ping struct{ from PID }
		echo := n.Spawn(func(s *Self) error {
			for {
				msg, err := s.Receive(context.Background())
				if err != nil {
					return err
				}
				s.Send(msg.(ping).from, msg)
			}
		})
		done := make(chan struct{})
		n.Spawn(func(s *Self) error {
			for b.Loop() {
				s.Send(echo, ping{s.PID()})
				if _, err := s.Receive(context.Background()); err != nil {
					return err
				}
			}
			close(done)
			return nil
		})
		<-done
	})
	b.Run("baseline", func(b *testing.B) {
		req, rep := make(chan int), make(chan int)
		go func() {
			for v := range req {
				rep <- v
			}
		}()
		for b.Loop() {
			req <- 1
			<-rep
		}
		close(req)
	})
}

func BenchmarkSendThroughput(b *testing.B) {
	b.Run("proc", func(b *testing.B) {
		n := NewNode("")
		done := make(chan struct{})
		count := b.N
		sink := n.Spawn(func(s *Self) error {
			for range count {
				if _, err := s.Receive(context.Background()); err != nil {
					return err
				}
			}
			close(done)
			return nil
		})
		b.ResetTimer()
		for i := range b.N {
			n.Send(sink, i)
		}
		<-done
	})
	b.Run("baseline", func(b *testing.B) {
		ch := make(chan any, 1024)
		done := make(chan struct{})
		count := b.N
		go func() {
			for range count {
				<-ch
			}
			close(done)
		}()
		b.ResetTimer()
		for i := range b.N {
			ch <- i
		}
		<-done
	})
}

func BenchmarkSpawn(b *testing.B) {
	b.Run("proc", func(b *testing.B) {
		n := NewNode("")
		var wg sync.WaitGroup
		for b.Loop() {
			wg.Add(1)
			n.Spawn(func(*Self) error { wg.Done(); return nil })
		}
		wg.Wait()
	})
	b.Run("baseline", func(b *testing.B) {
		var wg sync.WaitGroup
		for b.Loop() {
			wg.Go(func() {})
		}
		wg.Wait()
	})
}

// BenchmarkSendContended has all CPUs send to one process at once.
func BenchmarkSendContended(b *testing.B) {
	b.Run("proc", func(b *testing.B) {
		n := NewNode("")
		done := make(chan struct{})
		count := b.N
		sink := n.Spawn(func(s *Self) error {
			for range count {
				if _, err := s.Receive(context.Background()); err != nil {
					return err
				}
			}
			close(done)
			return nil
		})
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				n.Send(sink, 1)
			}
		})
		<-done
	})
	b.Run("baseline", func(b *testing.B) {
		ch := make(chan any, 1024)
		done := make(chan struct{})
		count := b.N
		go func() {
			for range count {
				<-ch
			}
			close(done)
		}()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ch <- 1
			}
		})
		<-done
	})
}
