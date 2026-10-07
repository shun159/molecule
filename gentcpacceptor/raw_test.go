package gentcpacceptor_test

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/internal/testlog"
	"github.com/shun159/molecule/proc"
)

// copyEcho is a raw handler writing back what it reads.
func copyEcho(_ *proc.Self, conn net.Conn) error {
	_, err := io.Copy(conn, conn)
	return err
}

// lines is a raw handler answering lines: "crash" panics, "quit" ends.
func lines(_ *proc.Self, conn net.Conn) error {
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		switch strings.TrimSpace(line) {
		case "crash":
			panic("crash requested")
		case "quit":
			io.WriteString(conn, "bye\n")
			return nil
		}
		io.WriteString(conn, line)
	}
}

func startRaw(t *testing.T, spec gentcpacceptor.Spec, h gentcpacceptor.Handler) (*proc.Node, *gentcpacceptor.Listener, *testlog.Recorder) {
	t.Helper()
	rec, logger := testlog.New()
	n := proc.NewNode("", proc.WithLogger(logger))
	spec.Addr = "127.0.0.1:0"
	l, err := gentcpacceptor.StartRaw(context.Background(), n, spec, h)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Stop(context.Background(), n) })
	return n, l, rec
}

func TestRawEcho(t *testing.T) {
	_, l, _ := startRaw(t, gentcpacceptor.Spec{}, copyEcho)
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			c := dial(t, l)
			for j := range 20 {
				want := strings.Repeat("y", i) + string(rune('a'+j))
				if got := c.send(want); got != want {
					t.Errorf("got %q, want %q", got, want)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestRawEnds(t *testing.T) {
	n, l, rec := startRaw(t, gentcpacceptor.Spec{}, lines)

	quit := dial(t, l)
	if got := quit.send("quit"); got != "bye" {
		t.Errorf("quit answered %q", got)
	}
	if !quit.closedByServer() {
		t.Error("connection left open after the handler returned")
	}

	reset := dial(t, l)
	reset.send("hi")
	reset.conn.(*net.TCPConn).SetLinger(0)
	reset.conn.Close()

	crash := dial(t, l)
	crash.send("hi")
	io.WriteString(crash.conn, "crash\n")
	if !crash.closedByServer() {
		t.Error("connection left open after a crash")
	}
	eventually(t, "the connection processes to be gone", func() bool { return count(t, n, l) == 0 })

	var crashes int
	for _, r := range rec.Records("") {
		if r.Level >= slog.LevelError && r.Message == "crash report" {
			crashes++
			if !strings.Contains(r.Attrs["reason"], "crash requested") {
				t.Errorf("crash report for %q", r.Attrs["reason"])
			}
		}
	}
	if crashes != 1 {
		t.Errorf("%d crash reports, want the panic only:\n%v", crashes, rec.Records(""))
	}
}

func TestRawStop(t *testing.T) {
	n := proc.NewNode("")
	l, err := gentcpacceptor.StartRaw(context.Background(), n, gentcpacceptor.Spec{Addr: "127.0.0.1:0"}, copyEcho)
	if err != nil {
		t.Fatal(err)
	}
	c := dial(t, l)
	c.send("hi") // the handler is now blocked in a read
	addr := l.Addr().String()
	if err := l.Stop(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if !c.closedByServer() {
		t.Error("connection left open after Stop")
	}
	if conn, err := net.Dial("tcp", addr); err == nil {
		conn.Close()
		t.Error("still accepting after Stop")
	}
}

func TestRawMaxConns(t *testing.T) {
	_, l, _ := startRaw(t, gentcpacceptor.Spec{MaxConns: 1}, copyEcho)
	first := dial(t, l)
	first.send("hi")
	if !dial(t, l).closedByServer() {
		t.Error("connection over MaxConns not closed")
	}
	if got := first.send("still"); got != "still" {
		t.Errorf("first connection answered %q", got)
	}
}
