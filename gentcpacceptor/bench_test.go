package gentcpacceptor_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/proc"
)

// Echo writes back what it reads.
type Echo struct{}

func (Echo) Init(gentcpacceptor.Socket) (struct{}, []gen.Effect, error) {
	return struct{}{}, nil, nil
}

func (Echo) HandleData(s struct{}, sock gentcpacceptor.Socket, b []byte) (struct{}, []gen.Effect) {
	return s, gen.Do(sock.Write(b))
}

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
	l, err := gentcpacceptor.Start(context.Background(), n, gentcpacceptor.Spec{Addr: "127.0.0.1:0"}, Echo{})
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

// BenchmarkEchoSize measures round trips of growing payloads over
// persistent connections, 4 per CPU, to see the fixed cost of each round
// trip amortized.
func BenchmarkEchoSize(b *testing.B) {
	for _, size := range []int{64, 1 << 10, 16 << 10, 256 << 10} {
		for _, srv := range servers {
			b.Run(fmt.Sprintf("%dB/%s", size, srv.name), func(b *testing.B) {
				addr := srv.start(b)
				b.SetParallelism(4)
				b.SetBytes(int64(size))
				b.RunParallel(func(pb *testing.PB) {
					conn, err := net.Dial("tcp", addr)
					if err != nil {
						b.Error(err)
						return
					}
					defer conn.Close()
					req, rep := make([]byte, size), make([]byte, size)
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
