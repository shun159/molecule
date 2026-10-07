package tcp_test

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/tcp"
)

// Echo is a connection handler that writes back what it reads.
type Echo struct{}

func (Echo) Init(any) (proc.PID, []gen.Effect, error) { return proc.PID{}, nil, nil }

func (Echo) Handle(sock proc.PID, msg gen.Msg) (proc.PID, []gen.Effect) {
	info, ok := msg.(gen.InfoMsg)
	if !ok {
		return sock, nil
	}
	switch m := info.Msg.(type) {
	case tcp.Attached:
		return m.Sock, gen.Do(tcp.ActiveOnce(m.Sock))
	case tcp.Data:
		return sock, gen.Do(tcp.Write(sock, m.Bytes), tcp.ActiveOnce(sock))
	case tcp.Closed:
		return sock, gen.Do(gen.Stop{})
	}
	return sock, nil
}

func (Echo) Terminate(proc.PID, error) []gen.Effect { return nil }

// The baseline is the usual Go echo server: a goroutine per connection.
func baselineEcho(b *testing.B) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 32<<10)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					if _, err := conn.Write(buf[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func moleculeEcho(b *testing.B) string {
	n := proc.NewNode("")
	l, err := tcp.Start(context.Background(), n, tcp.Spec{
		Addr:    "127.0.0.1:0",
		Handler: gen.StartLinkFunc(Echo{}, nil),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { l.Stop(context.Background(), n) })
	return l.Addr().String()
}

var servers = []struct {
	name  string
	start func(*testing.B) string
}{
	{"molecule", moleculeEcho},
	{"baseline", baselineEcho},
}

// BenchmarkEcho measures request-response round trips of 64 bytes over
// persistent connections, 4 per CPU.
func BenchmarkEcho(b *testing.B) {
	for _, srv := range servers {
		b.Run(srv.name, func(b *testing.B) {
			addr := srv.start(b)
			b.SetParallelism(4)
			b.SetBytes(64)
			b.RunParallel(func(pb *testing.PB) {
				conn, err := net.Dial("tcp", addr)
				if err != nil {
					b.Error(err)
					return
				}
				defer conn.Close()
				req, rep := make([]byte, 64), make([]byte, 64)
				for pb.Next() {
					if _, err := conn.Write(req); err != nil {
						b.Error(err)
						return
					}
					if _, err := io.ReadFull(conn, rep); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// BenchmarkConnect measures connection setup: dial, one round trip, close.
func BenchmarkConnect(b *testing.B) {
	for _, srv := range servers {
		b.Run(srv.name, func(b *testing.B) {
			addr := srv.start(b)
			b.RunParallel(func(pb *testing.PB) {
				buf := make([]byte, 1)
				for pb.Next() {
					conn, err := net.Dial("tcp", addr)
					if err != nil {
						b.Error(err)
						return
					}
					if _, err := conn.Write(buf); err == nil {
						_, err = io.ReadFull(conn, buf)
					}
					conn.Close()
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}
