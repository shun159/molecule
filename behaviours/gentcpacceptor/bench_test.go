package gentcpacceptor_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gentcpacceptor"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
)

// Echo writes back what it reads.
type Echo struct{}

func (Echo) Init(proc.PID, gentcp.Socket) (struct{}, []molecule.Effect, error) {
	return struct{}{}, nil, nil
}

func (Echo) HandleData(s struct{}, sock gentcp.Socket, b []byte) (struct{}, []molecule.Effect) {
	return s, molecule.Do(sock.SendEffect(b))
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

func moleculeRawEcho(b *testing.B) string {
	n := proc.NewNode("")
	l, err := gentcpacceptor.StartRaw(context.Background(), n, gentcpacceptor.Spec{Addr: "127.0.0.1:0"},
		func(_ *proc.Self, conn net.Conn) error {
			_, err := io.Copy(conn, conn)
			return err
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
	{"molecule-raw", moleculeRawEcho},
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

// summer checksums what it reads, standing for a handler with work to do
// on its data. The peer first sends the byte count to expect, as 8 bytes,
// and gets it back once all has arrived.
type summer struct{}

type sum struct {
	header []byte // the count, until all 8 bytes are in
	want   uint64
	n      uint64
	crc    uint32
}

func (summer) Init(proc.PID, gentcp.Socket) (sum, []molecule.Effect, error) {
	return sum{}, nil, nil
}

func (summer) HandleData(s sum, sock gentcp.Socket, b []byte) (sum, []molecule.Effect) {
	if len(s.header) < 8 {
		k := min(8-len(s.header), len(b))
		s.header = append(s.header[:len(s.header):len(s.header)], b[:k]...)
		b = b[k:]
		if len(s.header) == 8 {
			s.want = binary.BigEndian.Uint64(s.header)
		}
	}
	s.n += uint64(len(b))
	s.crc = work(s.crc, b)
	if len(s.header) == 8 && s.n == s.want {
		return s, molecule.Do(sock.SendEffect(s.header))
	}
	return s, nil
}

// work stands for processing the data.
func work(crc uint32, b []byte) uint32 {
	for range 4 {
		crc = crc32.Update(crc, crc32.IEEETable, b)
	}
	return crc
}

// BenchmarkStream sends 16 KB chunks one way over one connection to a
// handler that works on each, so reading ahead (Spec.ActiveN) can overlap
// reading with the work.
func BenchmarkStream(b *testing.B) {
	const chunk = 16 << 10
	baseline := func(b *testing.B) string {
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
					header := make([]byte, 8)
					if _, err := io.ReadFull(conn, header); err != nil {
						return
					}
					want := binary.BigEndian.Uint64(header)
					buf := make([]byte, 32<<10)
					var s sum
					for s.n < want {
						k, err := conn.Read(buf)
						s.n += uint64(k)
						s.crc = work(s.crc, buf[:k])
						if err != nil {
							return
						}
					}
					conn.Write(header)
				}()
			}
		}()
		return ln.Addr().String()
	}
	molecule := func(b *testing.B) string {
		n := proc.NewNode("")
		l, err := gentcpacceptor.Start(context.Background(), n, gentcpacceptor.Spec{Addr: "127.0.0.1:0"}, summer{})
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { l.Stop(context.Background(), n) })
		return l.Addr().String()
	}
	for _, srv := range []struct {
		name  string
		start func(*testing.B) string
	}{{"molecule", molecule}, {"baseline", baseline}} {
		b.Run(srv.name, func(b *testing.B) {
			addr := srv.start(b)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				b.Fatal(err)
			}
			defer conn.Close()
			want := uint64(b.N) * chunk
			if _, err := conn.Write(binary.BigEndian.AppendUint64(nil, want)); err != nil {
				b.Fatal(err)
			}
			b.SetBytes(chunk)
			buf := make([]byte, chunk)
			b.ResetTimer()
			for range b.N {
				if _, err := conn.Write(buf); err != nil {
					b.Fatal(err)
				}
			}
			var got [8]byte
			if _, err := io.ReadFull(conn, got[:]); err != nil {
				b.Fatal(err)
			}
			if n := binary.BigEndian.Uint64(got[:]); n != want {
				b.Fatalf("server counted %d bytes, want %d", n, want)
			}
		})
	}
}

// BenchmarkIdleConnMemory reports the memory an idle connection takes,
// heap and stack apart, client side included alike for both servers.
func BenchmarkIdleConnMemory(b *testing.B) {
	const conns = 1000
	for _, srv := range servers {
		b.Run(srv.name, func(b *testing.B) {
			addr := srv.start(b)
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)
			var cs []net.Conn
			for range conns {
				c, err := net.Dial("tcp", addr)
				if err != nil {
					b.Fatal(err)
				}
				cs = append(cs, c)
			}
			time.Sleep(time.Second)
			for range 5 { // stacks shrink by half per collection
				runtime.GC()
			}
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(after.HeapInuse-before.HeapInuse)/conns/1024, "heap-KB/conn")
			b.ReportMetric(float64(after.StackInuse-before.StackInuse)/conns/1024, "stack-KB/conn")
			for _, c := range cs {
				c.Close()
			}
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
