package main

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/dist"
)

// syncBuffer is a buffer written by the pinger and read by the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestPingPong runs both nodes in one program: a pings b, b goes away,
// and comes back as a new incarnation.
func TestPingPong(t *testing.T) {
	var mu sync.Mutex
	addrs := peers{}
	resolve := func(node string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		return addrs.resolve(node)
	}
	start := func(name string) *dist.Dist {
		t.Helper()
		_, d, err := startNode(name, dist.Config{Listen: "127.0.0.1:0", Cookie: "c", Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		t.Cleanup(d.Stop)
		return d
	}
	b := start("b@test")
	a, d, err := startNode("a@test", dist.Config{Cookie: "c", Resolve: resolve})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	var out syncBuffer
	a.Spawn(pinger(&out, "b@test", 50*time.Millisecond))

	waitFor(t, &out, 0, `pong #2 from b@test to "ping #2 from a@test"`)
	b.Stop()
	down := waitFor(t, &out, 0, "noconnection")
	start("b@test")
	waitFor(t, &out, down, "pong #1 from b@test")
}

// waitFor waits for s in out, after the first from bytes, and returns
// where it ends.
func waitFor(t *testing.T, out *syncBuffer, from int, s string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if i := strings.Index(out.String()[from:], s); i >= 0 {
			return from + i + len(s)
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %q in:\n%s", s, out.String()[from:])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
