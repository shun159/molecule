package gentcpacceptor_test

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
	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

// lineHandler answers each line: "crash" panics, "quit" stops normally,
// anything else is echoed. Lines are assumed to arrive whole, which holds
// for these small writes on loopback.
type lineHandler struct{}

func (lineHandler) Init(gentcpacceptor.Socket) (struct{}, []gen.Effect, error) {
	return struct{}{}, nil, nil
}

func (lineHandler) HandleData(s struct{}, sock gentcpacceptor.Socket, b []byte) (struct{}, []gen.Effect) {
	switch strings.TrimSpace(string(b)) {
	case "crash":
		panic("crash requested")
	case "quit":
		return s, gen.Do(sock.Write([]byte("bye\n")), gen.Stop{})
	}
	return s, gen.Do(sock.Write(b))
}

func start[S any](t *testing.T, spec gentcpacceptor.Spec, b gentcpacceptor.Behaviour[S]) (*proc.Node, *gentcpacceptor.Listener) {
	t.Helper()
	n := proc.NewNode("")
	spec.Addr = "127.0.0.1:0"
	l, err := gentcpacceptor.Start(context.Background(), n, spec, b)
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

func dial(t *testing.T, l *gentcpacceptor.Listener) *client {
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
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
		(err != nil && strings.Contains(err.Error(), "connection reset"))
}

func count(t *testing.T, n *proc.Node, l *gentcpacceptor.Listener) int {
	t.Helper()
	c, err := supervisor.CountChildren(context.Background(), n, l.Conns())
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// eventually polls cond, as the end of a connection reaches its process
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
	_, l := start(t, gentcpacceptor.Spec{Acceptors: 4}, lineHandler{})
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
	n, l := start(t, gentcpacceptor.Spec{}, lineHandler{})
	c := dial(t, l)
	c.send("hi")
	if got := count(t, n, l); got != 1 {
		t.Fatalf("connections = %d", got)
	}
	c.conn.Close()
	eventually(t, "the connection process to stop", func() bool { return count(t, n, l) == 0 })
}

// TestHandlerExitClosesConnection checks that the connection is closed
// whatever way its process ends, a normal exit included.
func TestHandlerExitClosesConnection(t *testing.T) {
	n, l := start(t, gentcpacceptor.Spec{}, lineHandler{})

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
	eventually(t, "the connection processes to be gone", func() bool { return count(t, n, l) == 0 })
}

func TestMaxConns(t *testing.T) {
	n, l := start(t, gentcpacceptor.Spec{MaxConns: 1}, lineHandler{})
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
	eventually(t, "the first connection process to stop", func() bool { return count(t, n, l) == 0 })
	if got := dial(t, l).send("again"); got != "again" {
		t.Errorf("after a slot freed up: %q", got)
	}
}

func TestStop(t *testing.T) {
	n := proc.NewNode("")
	l, err := gentcpacceptor.Start(context.Background(), n, gentcpacceptor.Spec{Addr: "127.0.0.1:0"}, lineHandler{})
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

// refuser fails Init, which must close the connection.
type refuser struct{}

func (refuser) Init(gentcpacceptor.Socket) (struct{}, []gen.Effect, error) {
	return struct{}{}, nil, errors.New("go away")
}

func (refuser) HandleData(s struct{}, _ gentcpacceptor.Socket, _ []byte) (struct{}, []gen.Effect) {
	return s, nil
}

func TestInitError(t *testing.T) {
	n, l := start(t, gentcpacceptor.Spec{}, refuser{})
	if !dial(t, l).closedByServer() {
		t.Error("connection left open after Init failed")
	}
	eventually(t, "the connection process to stop", func() bool { return count(t, n, l) == 0 })
}

// greeter tells the peer its own address, which Init gets with the socket.
type greeter struct{}

func (greeter) Init(sock gentcpacceptor.Socket) (struct{}, []gen.Effect, error) {
	return struct{}{}, gen.Do(sock.Write([]byte(sock.RemoteAddr.String() + "\n"))), nil
}

func (greeter) HandleData(s struct{}, _ gentcpacceptor.Socket, _ []byte) (struct{}, []gen.Effect) {
	return s, nil
}

func TestSocketAddrs(t *testing.T) {
	_, l := start(t, gentcpacceptor.Spec{}, greeter{})
	c := dial(t, l)
	got, err := c.r.ReadString('\n')
	if err != nil || strings.TrimSpace(got) != c.conn.LocalAddr().String() {
		t.Errorf("greeting = %q, %v; want %v", got, err, c.conn.LocalAddr())
	}
}
