package gentcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

func TestNextReadSize(t *testing.T) {
	for _, tt := range []struct{ size, n, want int }{
		{minReadBuffer, minReadBuffer, 2 * minReadBuffer}, // full: grow
		{maxReadBuffer, maxReadBuffer, maxReadBuffer},     // full at the top: stay
		{8 << 10, 4 << 10, 8 << 10},                       // half: stay
		{8 << 10, 1 << 10, 4 << 10},                       // under a quarter: shrink
		{minReadBuffer, 0, minReadBuffer},                 // at the bottom: stay
		{16 << 10, 16<<10 - 1, 16 << 10},                  // nearly full: stay
	} {
		if got := nextReadSize(tt.size, tt.n); got != tt.want {
			t.Errorf("nextReadSize(%d, %d) = %d, want %d", tt.size, tt.n, got, tt.want)
		}
	}
}

// TestHandsOverFullReads reads chunks filling the buffer, which are handed
// over without a copy: the next read must go to a new buffer.
func TestHandsOverFullReads(t *testing.T) {
	n := proc.NewNode("")
	got := make(chan DataMsg, 2)
	owner := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			if m, ok := msg.(DataMsg); ok {
				got <- m
			}
		}
	})
	server, peer := net.Pipe()
	defer peer.Close()
	Start(n, server, owner, Options{Active: N(2)})
	chunks := [][]byte{bytes.Repeat([]byte("a"), minReadBuffer), bytes.Repeat([]byte("b"), minReadBuffer)}
	go func() {
		for _, c := range chunks {
			peer.Write(c)
		}
	}()
	var recvd [][]byte
	for range chunks {
		select {
		case m := <-got:
			recvd = append(recvd, m.Bytes)
		case <-time.After(5 * time.Second):
			t.Fatal("nothing delivered")
		}
	}
	if !bytes.Equal(recvd[0], chunks[0]) || !bytes.Equal(recvd[1], chunks[1]) {
		t.Errorf("first read now %q..., second %q...", recvd[0][:4], recvd[1][:4])
	}
}

// TestSendActiveFailed fails the send of a passive socket, made by the
// socket process for a Socket without writer: the mode still changes, so
// that the owner hears of the failure.
func TestSendActiveFailed(t *testing.T) {
	n := proc.NewNode("")
	got := make(chan any, 4)
	owner := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg
		}
	})
	server, peer := net.Pipe()
	defer peer.Close()
	// The write deadline has passed before the write starts.
	sock := Start(n, server, owner, Options{SendTimeout: time.Nanosecond})
	byHand := Socket{PID: sock.PID}
	n.Send(sock.PID, byHand.SendActiveEffect([]byte("x"), Once).(molecule.Send).Msg)
	for _, want := range []string{"ErrorMsg", "ClosedMsg"} {
		select {
		case m := <-got:
			switch m := m.(type) {
			case ErrorMsg:
				if want != "ErrorMsg" || !errors.Is(m.Err, os.ErrDeadlineExceeded) {
					t.Errorf("got %#v, want %s", m, want)
				}
			case ClosedMsg:
				if want != "ClosedMsg" {
					t.Errorf("got %#v, want %s", m, want)
				}
			default:
				t.Errorf("got %#v, want %s", m, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no %s", want)
		}
	}
}

// TestDirectOrder stresses the reads the reader delivers itself: in each
// round, the owner gets the packet, at once asks for one more, and must
// get the Passive message of the round, after the packet. The
// reader must consume the allowance before the request for more, and
// send Passive after the packet.
func TestDirectOrder(t *testing.T) {
	n := proc.NewNode("")
	server, peer := net.Pipe()
	defer peer.Close()
	const rounds = 2000
	next := make(chan struct{})
	failed := make(chan string, 1)
	owner := n.Spawn(func(s *proc.Self) error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for i := range rounds {
			msg, err := s.Receive(ctx)
			d, ok := msg.(DataMsg)
			if err != nil || !ok {
				failed <- fmt.Sprintf("round %d: got %#v, %v; want data", i, msg, err)
				return nil
			}
			s.Send(d.Sock.PID, setActiveReq{N(1)})
			if msg, err := s.Receive(ctx); err != nil || msg != (PassiveMsg{d.Sock}) {
				failed <- fmt.Sprintf("round %d: got %#v, %v; want passive", i, msg, err)
				return nil
			}
			next <- struct{}{}
		}
		close(failed)
		return nil
	})
	Start(n, server, owner, Options{Active: N(1)})
	go func() {
		for range rounds {
			if _, err := peer.Write([]byte("x")); err != nil {
				return
			}
			select {
			case <-next:
			case <-time.After(20 * time.Second):
				return
			}
		}
	}()
	if msg, ok := <-failed; ok {
		t.Fatal(msg)
	}
}

// countConn counts the writes to a connection.
type countConn struct {
	net.Conn
	mu     sync.Mutex
	writes int
}

func (c *countConn) Write(b []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	c.mu.Unlock()
	return c.Conn.Write(b)
}

// TestOneWritePerPacket sends a framed packet on a connection other than
// TCP, such as TLS: header and data go in one write.
func TestOneWritePerPacket(t *testing.T) {
	n := proc.NewNode("")
	server, peer := net.Pipe()
	defer peer.Close()
	conn := &countConn{Conn: server}
	owner := n.Spawn(func(s *proc.Self) error {
		_, err := s.Receive(context.Background())
		return err
	})
	sock := Start(n, conn, owner, Options{Packet: Packet4})
	go io.Copy(io.Discard, peer)
	if err := sock.Send(context.Background(), n, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.writes != 1 {
		t.Errorf("%d writes", conn.writes)
	}
}

// Raw active modes must stop reading when their allowance is exhausted,
// including when the reader itself consumes the last allowance.
func TestRawActiveBackpressure(t *testing.T) {
	for _, tt := range []struct {
		name    string
		active  Active
		packets int
	}{
		{"once", Once, 1},
		{"count", N(2), 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := proc.NewNode("")
			got := make(chan any, 4)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			owner := n.Spawn(func(s *proc.Self) error {
				for {
					msg, err := s.Receive(ctx)
					if err != nil {
						return nil
					}
					got <- msg
				}
			})
			server, peer := net.Pipe()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(5 * time.Second))
			sock := Start(n, server, owner, Options{Active: tt.active})
			defer sock.Close(ctx, n)
			receive := func() any {
				t.Helper()
				select {
				case m := <-got:
					return m
				case <-ctx.Done():
					t.Fatal("no socket message")
					return nil
				}
			}
			for range tt.packets {
				if _, err := peer.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
				if m, ok := receive().(DataMsg); !ok || string(m.Bytes) != "x" {
					t.Fatalf("got %#v, want data", m)
				}
			}
			if tt.active.kind == count {
				if m := receive(); m != (PassiveMsg{sock}) {
					t.Fatalf("got %#v, want passive", m)
				}
			}
			// A pipe write completes only if a Read consumes it. With no
			// allowance, the next write must time out without consuming bytes.
			peer.SetWriteDeadline(time.Now().Add(30 * time.Millisecond))
			if k, err := peer.Write([]byte("y")); k != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("passive write = %d, %v; reader advanced without permission", k, err)
			}
			peer.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := sock.SetActive(ctx, n, Once); err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Write([]byte("z")); err != nil {
				t.Fatal(err)
			}
			if m, ok := receive().(DataMsg); !ok || string(m.Bytes) != "z" {
				t.Fatalf("after reactivation: got %#v, want z", m)
			}
		})
	}
}

// transferRequest asks an owner to give its socket to heir.
type transferRequest struct {
	sock Socket
	heir proc.PID
	done chan error
}

// readStartedConn tells when a Read starts, so that the owner can change
// during a Read the reader went on to by itself, rather than only during
// the first, which the process asked for.
type readStartedConn struct {
	net.Conn
	started chan struct{}
}

func (c *readStartedConn) Read(b []byte) (int, error) {
	select {
	case c.started <- struct{}{}:
	default: // no one waits for this one
	}
	return c.Conn.Read(b)
}

func TestRawContinuousOwnerChange(t *testing.T) {
	for _, tt := range []struct {
		name   string
		active Active
	}{
		{"always", Always}, {"count", N(2)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			n := proc.NewNode("")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			spawn := func(got chan any) proc.PID {
				return n.Spawn(func(s *proc.Self) error {
					for {
						msg, err := s.Receive(ctx)
						if err != nil {
							return nil
						}
						if req, ok := msg.(transferRequest); ok {
							req.done <- req.sock.ControllingProcess(ctx, s, req.heir)
							continue
						}
						got <- msg
					}
				})
			}
			oldMessages, newMessages := make(chan any, 4), make(chan any, 4)
			owner, heir := spawn(oldMessages), spawn(newMessages)
			server, peer := net.Pipe()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(5 * time.Second))
			c := &readStartedConn{Conn: server, started: make(chan struct{}, 4)}
			sock := Start(n, c, owner, Options{Active: tt.active})
			defer sock.Close(ctx, n)
			awaitRead := func() {
				t.Helper()
				select {
				case <-c.started:
				case <-ctx.Done():
					t.Fatal("no next Read")
				}
			}
			wantData := func(ch chan any, want string) {
				t.Helper()
				select {
				case m := <-ch:
					if d, ok := m.(DataMsg); !ok || string(d.Bytes) != want {
						t.Fatalf("got %#v, want %s", m, want)
					}
				case <-ctx.Done():
					t.Fatal("no data")
				}
			}
			awaitRead()
			if _, err := peer.Write([]byte("a")); err != nil {
				t.Fatal(err)
			}
			wantData(oldMessages, "a")
			awaitRead()
			done := make(chan error, 1)
			n.Send(owner, transferRequest{sock, heir, done})
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("owner transfer did not finish")
			}
			if _, err := peer.Write([]byte("b")); err != nil {
				t.Fatal(err)
			}
			wantData(newMessages, "b")
			if tt.active.kind == count {
				select {
				case m := <-newMessages:
					if m != (PassiveMsg{sock}) {
						t.Fatalf("got %#v, want passive", m)
					}
				case <-ctx.Done():
					t.Fatal("no passive message")
				}
			}
			// EOF must follow the data (and the count's PassiveMsg) at
			// the new owner. Exhausted N needs a new allowance to see EOF.
			if tt.active.kind == count {
				if err := sock.SetActive(ctx, n, Once); err != nil {
					t.Fatal(err)
				}
			}
			peer.Close()
			select {
			case m := <-newMessages:
				if m != (ClosedMsg{sock}) {
					t.Fatalf("got %#v, want closed", m)
				}
			case <-ctx.Done():
				t.Fatal("no closed message")
			}
			select {
			case m := <-oldMessages:
				t.Fatalf("old owner got %#v after transfer", m)
			default:
			}
		})
	}
}
