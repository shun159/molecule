package tcp_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
	"github.com/shun159/molecule/tcp"
)

// lineHandler answers each line: "crash" panics, "quit" stops normally,
// anything else is echoed. Lines are assumed to arrive whole, which holds
// for these small writes on loopback.
type lineHandler struct{}

func (lineHandler) Init(any) (proc.PID, []gen.Effect, error) { return proc.PID{}, nil, nil }

func (lineHandler) Handle(sock proc.PID, msg gen.Msg) (proc.PID, []gen.Effect) {
	info, ok := msg.(gen.InfoMsg)
	if !ok {
		return sock, nil
	}
	switch m := info.Msg.(type) {
	case tcp.Attached:
		return m.Sock, gen.Do(tcp.ActiveOnce(m.Sock))
	case tcp.Data:
		switch strings.TrimSpace(string(m.Bytes)) {
		case "crash":
			panic("crash requested")
		case "quit":
			return sock, gen.Do(tcp.Write(sock, []byte("bye\n")), gen.Stop{})
		}
		return sock, gen.Do(tcp.Write(sock, m.Bytes), tcp.ActiveOnce(sock))
	case tcp.Closed:
		return sock, gen.Do(gen.Stop{})
	}
	return sock, nil
}

func (lineHandler) Terminate(proc.PID, error) []gen.Effect { return nil }

func start(t *testing.T, spec tcp.Spec) (*proc.Node, *tcp.Listener) {
	t.Helper()
	n := proc.NewNode("")
	spec.Addr = "127.0.0.1:0"
	if spec.Handler == nil {
		spec.Handler = gen.StartLinkFunc(lineHandler{}, nil)
	}
	l, err := tcp.Start(context.Background(), n, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Stop(context.Background(), n) })
	return n, l
}

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, l *tcp.Listener) *client {
	t.Helper()
	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &client{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func (c *client) send(line string) string {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, line+"\n"); err != nil {
		c.t.Fatal(err)
	}
	got, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("reading the answer to %q: %v", line, err)
	}
	return strings.TrimSuffix(got, "\n")
}

// closedByServer reports whether the server closed the connection.
func (c *client) closedByServer() bool {
	c.t.Helper()
	_, err := c.r.ReadByte()
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || isReset(err)
}

func isReset(err error) bool {
	return err != nil && strings.Contains(err.Error(), "connection reset")
}

func count(t *testing.T, n *proc.Node, l *tcp.Listener) int {
	t.Helper()
	c, err := supervisor.CountChildren(context.Background(), n, l.Conns())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// eventually polls cond, as the end of a connection reaches the handler
// asynchronously.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEchoConcurrent(t *testing.T) {
	_, l := start(t, tcp.Spec{Acceptors: 4})
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			c := dial(t, l)
			for j := range 50 {
				want := strings.Repeat("x", i) + string(rune('a'+j%26))
				if got := c.send(want); got != want {
					t.Errorf("got %q, want %q", got, want)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestClientClose(t *testing.T) {
	n, l := start(t, tcp.Spec{})
	c := dial(t, l)
	c.send("hi")
	if got := count(t, n, l); got != 1 {
		t.Fatalf("handlers = %d", got)
	}
	c.conn.Close()
	eventually(t, "the handler to stop", func() bool { return count(t, n, l) == 0 })
}

// TestHandlerExitClosesConnection checks that the connection is closed
// whatever way its handler ends, a normal exit included.
func TestHandlerExitClosesConnection(t *testing.T) {
	n, l := start(t, tcp.Spec{})

	quit := dial(t, l)
	if got := quit.send("quit"); got != "bye" {
		t.Errorf("quit answered %q", got)
	}
	if !quit.closedByServer() {
		t.Error("connection left open after a normal exit")
	}

	crash := dial(t, l)
	crash.send("hi")
	io.WriteString(crash.conn, "crash\n")
	if !crash.closedByServer() {
		t.Error("connection left open after a crash")
	}
	eventually(t, "the handlers to be gone", func() bool { return count(t, n, l) == 0 })
}

func TestMaxConns(t *testing.T) {
	n, l := start(t, tcp.Spec{MaxConns: 1})
	first := dial(t, l)
	first.send("hi")

	second := dial(t, l)
	if !second.closedByServer() {
		t.Error("connection over MaxConns not closed")
	}
	if got := first.send("still here"); got != "still here" {
		t.Errorf("first connection answered %q", got)
	}

	first.conn.Close()
	eventually(t, "the first handler to stop", func() bool { return count(t, n, l) == 0 })
	if got := dial(t, l).send("again"); got != "again" {
		t.Errorf("after a slot freed up: %q", got)
	}
}

func TestStop(t *testing.T) {
	n := proc.NewNode("")
	l, err := tcp.Start(context.Background(), n, tcp.Spec{Addr: "127.0.0.1:0", Handler: gen.StartLinkFunc(lineHandler{}, nil)})
	if err != nil {
		t.Fatal(err)
	}
	c := dial(t, l)
	c.send("hi")
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

// TestActiveOnce checks that a handler that does not ask for more data
// gets none: the socket reads only on demand.
func TestActiveOnce(t *testing.T) {
	got := make(chan []byte, 16)
	handler := gen.StartLinkFunc(onceHandler{got}, nil)
	_, l := start(t, tcp.Spec{Handler: handler})
	c := dial(t, l)

	io.WriteString(c.conn, "first")
	if b := <-got; string(b) != "first" {
		t.Fatalf("got %q", b)
	}
	io.WriteString(c.conn, "second")
	select {
	case b := <-got:
		t.Errorf("data delivered without ActiveOnce: %q", b)
	case <-time.After(100 * time.Millisecond):
	}
}

// onceHandler asks for data once only, and reports what it gets.
type onceHandler struct{ got chan<- []byte }

func (onceHandler) Init(any) (struct{}, []gen.Effect, error) { return struct{}{}, nil, nil }

func (h onceHandler) Handle(s struct{}, msg gen.Msg) (struct{}, []gen.Effect) {
	info, _ := msg.(gen.InfoMsg)
	switch m := info.Msg.(type) {
	case tcp.Attached:
		return s, gen.Do(tcp.ActiveOnce(m.Sock))
	case tcp.Data:
		h.got <- m.Bytes // a test probe, not something a real handler does
	}
	return s, nil
}

func (onceHandler) Terminate(struct{}, error) []gen.Effect { return nil }
