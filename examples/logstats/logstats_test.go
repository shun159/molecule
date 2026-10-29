package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/behaviours/stage"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

func TestParseLine(t *testing.T) {
	cases := []struct {
		line string
		want record
		ok   bool
	}{
		{"/a 12", record{"/a", 12}, true},
		{"  /a  ", record{"/a", 1}, true},
		{"/a x", record{}, false},
		{"", record{}, false},
	}
	for _, c := range cases {
		got, ok := parseLine(c.line)
		if ok != c.ok || ok && got != c.want {
			t.Errorf("%q: %+v %v", c.line, got, ok)
		}
	}
}

func TestRecordCodec(t *testing.T) {
	r := record{Key: "/api/item/7", Value: -42}
	b, _ := r.AppendBinary(nil)
	var got record
	if err := got.DecodeBinary(b); err != nil || got != r {
		t.Fatalf("%+v, %v", got, err)
	}
	if err := got.DecodeBinary(b[:len(b)-1]); err == nil {
		t.Fatal("read cut short")
	}
}

// TestReadLines reads complete lines only, from the end at first, and
// from the start of a file cut shorter.
func TestReadLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	if r, err := readLines(path, 0, 10); err != nil || len(r.lines) != 0 {
		t.Fatalf("a file not there: %+v %v", r, err)
	}
	os.WriteFile(path, []byte("a 1\nb 2\nc"), 0o644)
	if r, _ := readLines(path, -1, 10); r.offset != 9 || len(r.lines) != 0 {
		t.Fatalf("from the end: %+v", r)
	}
	r, _ := readLines(path, 0, 10)
	if len(r.lines) != 2 || r.offset != 8 {
		t.Fatalf("a line being written read: %+v", r)
	}
	if r, _ := readLines(path, 0, 1); len(r.lines) != 1 || r.offset != 4 {
		t.Fatalf("one line asked for: %+v", r)
	}
	os.WriteFile(path, []byte("d 4\n"), 0o644)
	if r, _ := readLines(path, 8, 10); len(r.lines) != 1 || r.lines[0] != "d 4" {
		t.Fatalf("a file cut shorter: %+v", r)
	}
}

// lockedBuffer is a buffer written by the reducers of all the nodes.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

var reported = regexp.MustCompile(`partition \d: (\d+) records`)

// counted is the records the windows written so far counted.
func (l *lockedBuffer) counted() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, m := range reported.FindAllStringSubmatch(l.b.String(), -1) {
		c, _ := strconv.Atoi(m[1])
		n += c
	}
	return n
}

// subscribed waits until each of the parsers of each node has consumers,
// the reducers of all the nodes: records parsed before are dropped.
func subscribed(t *testing.T, nodes []*proc.Node, parsers, consumers int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, n := range nodes {
		for i := range parsers {
			for {
				st, err := stage.StatsOf(ctx, n, parseName(i))
				if err == nil && st.Consumers == consumers {
					break
				}
				if ctx.Err() != nil {
					t.Fatalf("%s %s: %+v, %v", n.Name(), parseName(i), st, err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// TestTwoNodes has two nodes tail their files: every line written to
// either is counted once, by the node of its key.
func TestTwoNodes(t *testing.T) {
	dir := t.TempDir()
	names := []string{"a@logstats", "b@logstats"}
	addrs := map[string]string{"a@logstats": "127.0.0.1:0", "b@logstats": "127.0.0.1:0"}
	var mu sync.Mutex
	resolve := func(node string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		a, ok := addrs[node]
		return a, ok
	}
	out := &lockedBuffer{}
	var nodes []*proc.Node
	for i, name := range names {
		n := proc.NewNode(name)
		d, err := dist.Start(n, dist.Config{Listen: "127.0.0.1:0", Cookie: "logstats", Resolve: resolve})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(d.Stop)
		mu.Lock()
		addrs[name] = d.Addr().String()
		mu.Unlock()
		c := config{file: filepath.Join(dir, name), fromStart: true, parsers: 2, nodes: names, partition: i,
			window: 100 * time.Millisecond, top: 3, out: out}
		app, err := startLogstats(context.Background(), n, c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { app.Stop(context.Background()) })
		nodes = append(nodes, n)
	}
	subscribed(t, nodes, 2, len(names))

	const lines = 5000
	for _, name := range names {
		var b []byte
		for i := range lines {
			b = fmt.Appendf(b, "/key/%d %d\n", i%97, i)
		}
		os.WriteFile(filepath.Join(dir, name), b, 0o644)
	}
	deadline := time.Now().Add(10 * time.Second)
	for out.counted() < 2*lines && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n := out.counted(); n != 2*lines {
		t.Fatalf("%d records counted of %d", n, 2*lines)
	}
}
