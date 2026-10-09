package main

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/shun159/molecule/proc"
)

// BenchmarkEchoRoundTrip compares the example with a goroutine doing
// Read/Write directly, over one persistent TCP connection with 64-byte
// requests. Startup and connection teardown are outside the measured loop.
func BenchmarkEchoRoundTrip(b *testing.B) {
	for _, mode := range []string{"go", "molecule"} {
		b.Run(mode, func(b *testing.B) {
			var addr string
			if mode == "molecule" {
				app, a, err := startEcho(context.Background(), proc.NewNode(""), "127.0.0.1:0")
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { app.Stop(context.Background()) })
				addr = a.String()
			} else {
				addr = startGoEcho(b)
			}
			c, err := net.Dial("tcp", addr)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Minute))
			send, recv := make([]byte, 64), make([]byte, 64)
			roundTrip := func() {
				if _, err := c.Write(send); err != nil {
					b.Fatal(err)
				}
				if _, err := io.ReadFull(c, recv); err != nil {
					b.Fatal(err)
				}
			}
			roundTrip() // warm the protocol, reader buffer, and mailboxes
			b.ReportAllocs()
			for b.Loop() {
				roundTrip()
			}
		})
	}
}

func startGoEcho(b *testing.B) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 2<<10)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						if _, err := c.Write(buf[:n]); err != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}
