package gentcp_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
)

// inProc runs fn in a process, and waits for it.
func inProc(t *testing.T, n *proc.Node, fn func(s *proc.Self)) {
	t.Helper()
	done := make(chan struct{})
	n.Spawn(func(s *proc.Self) error {
		defer close(done)
		fn(s)
		return nil
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("test process did not finish")
	}
}

// pair connects a socket owned by s to a plain connection.
func pair(t *testing.T, s *proc.Self, opts gentcp.Options) (gentcp.Socket, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	peer := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		peer <- c
	}()
	sock, err := gentcp.Connect(context.Background(), s, ln.Addr().String(), opts)
	if err != nil {
		t.Fatal(err)
	}
	c := <-peer
	t.Cleanup(func() { c.Close() })
	return sock, c
}

// receive takes the next message of s.
func receive(t *testing.T, s *proc.Self) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := s.Receive(ctx)
	if err != nil {
		t.Errorf("no message: %v", err)
	}
	return msg
}

// quiet checks that s gets no message for a while.
func quiet(t *testing.T, s *proc.Self) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if msg, err := s.Receive(ctx); err == nil {
		t.Errorf("unexpected message %#v", msg)
	}
}

func wantData(t *testing.T, s *proc.Self, sock gentcp.Socket, want string) {
	t.Helper()
	switch m := receive(t, s).(type) {
	case gentcp.DataMsg:
		if m.Sock != sock || string(m.Bytes) != want {
			t.Errorf("got data %q from %v, want %q from %v", m.Bytes, m.Sock.PID, want, sock.PID)
		}
	default:
		t.Errorf("got %#v, want data %q", m, want)
	}
}

func wantRecv(t *testing.T, s *proc.Self, sock gentcp.Socket, length int, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := sock.Recv(ctx, s, length)
	if err != nil || string(got) != want {
		t.Errorf("Recv: %q, %v; want %q", got, err, want)
	}
}

func readAll(t *testing.T, c net.Conn, n int) string {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	b := make([]byte, n)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Errorf("peer read: %v", err)
	}
	return string(b)
}

func wantEOF(t *testing.T, c net.Conn) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	if k, err := c.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("peer read: %d, %v; want EOF", k, err)
	}
}

func frame4(s string) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(len(s)))
}

func TestEcho(t *testing.T) {
	n := proc.NewNode("")
	opts := gentcp.Options{Packet: gentcp.Packet4}
	inProc(t, n, func(s *proc.Self) {
		ctx := context.Background()
		ls, err := gentcp.Listen(ctx, s, "127.0.0.1:0", opts)
		if err != nil {
			t.Fatal(err)
		}
		// The server echoes in passive mode until the client closes.
		n.Spawn(func(srv *proc.Self) error {
			sock, err := ls.Accept(ctx, srv)
			if err != nil {
				t.Error(err)
				return err
			}
			for {
				b, err := sock.Recv(ctx, srv, 0)
				if err != nil {
					return nil
				}
				if err := sock.Send(ctx, srv, b); err != nil {
					t.Error(err)
				}
			}
		})
		c, err := gentcp.Connect(ctx, s, ls.Addr().String(), opts)
		if err != nil {
			t.Fatal(err)
		}
		for _, msg := range []string{"hello", "", string(bytes.Repeat([]byte("x"), 100000))} {
			if err := c.Send(ctx, s, []byte(msg)); err != nil {
				t.Error(err)
			}
			wantRecv(t, s, c, 0, msg)
		}
		if err := c.Close(ctx, s); err != nil {
			t.Error(err)
		}
		if err := c.Close(ctx, s); err != nil {
			t.Errorf("second Close: %v", err)
		}
		if err := c.Send(ctx, s, []byte("x")); err != gentcp.ErrClosed {
			t.Errorf("Send after Close: %v", err)
		}
		if err := ls.Close(ctx, s); err != nil {
			t.Error(err)
		}
		if _, err := ls.Accept(ctx, s); err != gentcp.ErrClosed {
			t.Errorf("Accept after Close: %v", err)
		}
	})
}

func TestActiveOnce(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Line, Active: gentcp.Once})
		io.WriteString(peer, "a\nb\n")
		wantData(t, s, sock, "a\n")
		quiet(t, s)
		sock.SetActive(context.Background(), s, gentcp.Once)
		wantData(t, s, sock, "b\n")
		quiet(t, s)
	})
}

func TestActiveN(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Line, Active: gentcp.N(2)})
		io.WriteString(peer, "a\nb\nc\n")
		wantData(t, s, sock, "a\n")
		wantData(t, s, sock, "b\n")
		if m, ok := receive(t, s).(gentcp.PassiveMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Passive", m)
		}
		quiet(t, s)
		// Passive, the socket receives on Recv.
		wantRecv(t, s, sock, 0, "c\n")
		// Counts add up; reaching zero turns the socket passive.
		ctx := context.Background()
		sock.SetActive(ctx, s, gentcp.N(1))
		sock.SetActive(ctx, s, gentcp.N(-1))
		if m, ok := receive(t, s).(gentcp.PassiveMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Passive", m)
		}
		io.WriteString(peer, "d\n")
		quiet(t, s)
		wantRecv(t, s, sock, 0, "d\n")
	})
}

func TestActiveAlways(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Line, Active: gentcp.Always})
		if _, err := sock.Recv(context.Background(), s, 0); err != gentcp.ErrActive {
			t.Errorf("Recv on an active socket: %v", err)
		}
		io.WriteString(peer, "a\nb")
		io.WriteString(peer, "\nc")
		peer.Close()
		wantData(t, s, sock, "a\n")
		wantData(t, s, sock, "b\n")
		// "c" is not a whole line: the connection ends without it.
		if m, ok := receive(t, s).(gentcp.ClosedMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Closed", m)
		}
		awaitExit(t, s, sock)
	})
}

func awaitExit(t *testing.T, s *proc.Self, sock gentcp.Socket) {
	t.Helper()
	ctx, cancel := s.Node().Watch(context.Background(), sock.PID)
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Error("socket process still alive")
	}
}

func TestPacket4(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Packet4})
		// A packet arriving a byte at a time is received whole.
		msg := append(frame4("hello"), "hello"...)
		go func() {
			for _, b := range msg {
				peer.Write([]byte{b})
				time.Sleep(time.Millisecond)
			}
		}()
		wantRecv(t, s, sock, 0, "hello")

		// Sending adds the header.
		sock.Send(context.Background(), s, []byte("hi"))
		if got := readAll(t, peer, 6); got != string(frame4("hi"))+"hi" {
			t.Errorf("peer got %q", got)
		}
	})
}

func TestRawLength(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{})
		go func() {
			io.WriteString(peer, "abc")
			time.Sleep(10 * time.Millisecond)
			io.WriteString(peer, "defg")
		}()
		wantRecv(t, s, sock, 5, "abcde")
		wantRecv(t, s, sock, 0, "fg")
	})
}

func TestPacketTooLarge(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Packet4, PacketSize: 4, Active: gentcp.Once})
		peer.Write(append(frame4("hello"), "hello"...))
		if m, ok := receive(t, s).(gentcp.ErrorMsg); !ok || m.Sock != sock || !errors.Is(m.Err, gentcp.ErrPacketTooLarge) {
			t.Errorf("got %#v, want Error too large", m)
		}
		if m, ok := receive(t, s).(gentcp.ClosedMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Closed", m)
		}
		awaitExit(t, s, sock)
		wantEOF(t, peer)

		// Sending is checked as well.
		sock, _ = pair(t, s, gentcp.Options{Packet: gentcp.Packet1})
		err := sock.Send(context.Background(), s, make([]byte, 256))
		if !errors.Is(err, gentcp.ErrPacketTooLarge) {
			t.Errorf("Send: %v", err)
		}
	})
}

func TestOwnerExit(t *testing.T) {
	n := proc.NewNode("")
	var peer net.Conn
	inProc(t, n, func(s *proc.Self) {
		_, peer = pair(t, s, gentcp.Options{})
	})
	wantEOF(t, peer)
}

func TestControllingProcess(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	got := make(chan error, 1)
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Line})
		ready := make(chan struct{})
		heir := n.Spawn(func(h *proc.Self) error {
			<-ready
			if err := sock.SetActive(ctx, h, gentcp.Once); err != nil {
				t.Error(err)
			}
			wantData(t, h, sock, "a\n")
			got <- sock.Send(ctx, h, []byte("b\n"))
			return nil
		})
		if err := sock.ControllingProcess(ctx, s, heir); err != nil {
			t.Error(err)
		}
		if err := sock.ControllingProcess(ctx, s, s.PID()); err != gentcp.ErrNotOwner {
			t.Errorf("ControllingProcess by the old owner: %v", err)
		}
		if _, err := sock.Recv(ctx, s, 0); err != gentcp.ErrNotOwner {
			t.Errorf("Recv by the old owner: %v", err)
		}
		close(ready)
		io.WriteString(peer, "a\n")
		// The socket outlives its first owner, which exits now.
		t.Cleanup(func() {
			if err := <-got; err != nil {
				t.Errorf("Send by the heir: %v", err)
			}
			if got := readAll(t, peer, 2); got != "b\n" {
				t.Errorf("peer got %q", got)
			}
		})
	})
}

func TestHalfClosed(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Active: gentcp.Always, HalfClosed: true})
		io.WriteString(peer, "bye")
		peer.(*net.TCPConn).CloseWrite()
		wantData(t, s, sock, "bye")
		if m, ok := receive(t, s).(gentcp.ClosedMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Closed", m)
		}
		// Closed for reading, the socket still sends.
		if err := sock.Send(ctx, s, []byte("ok")); err != nil {
			t.Error(err)
		}
		if got := readAll(t, peer, 2); got != "ok" {
			t.Errorf("peer got %q", got)
		}
		sock.Close(ctx, s)
		wantEOF(t, peer)
	})
}

func TestShutdownWrite(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{})
		sock.Send(ctx, s, []byte("req"))
		if err := sock.Shutdown(ctx, s, gentcp.Write); err != nil {
			t.Error(err)
		}
		if got := readAll(t, peer, 3); got != "req" {
			t.Errorf("peer got %q", got)
		}
		wantEOF(t, peer)
		// The peer still sends, and the socket still receives.
		io.WriteString(peer, "resp")
		wantRecv(t, s, sock, 4, "resp")
	})
}

func TestRecvTimeout(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := sock.Recv(ctx, s, 0); err != gentcp.ErrTimeout {
			t.Errorf("Recv: %v, want timeout", err)
		}
		// A partial read times out, and the data waits for the next Recv.
		io.WriteString(peer, "ab")
		ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := sock.Recv(ctx, s, 3); err != gentcp.ErrTimeout {
			t.Errorf("Recv: %v, want timeout", err)
		}
		io.WriteString(peer, "c")
		wantRecv(t, s, sock, 3, "abc")
	})
}

func TestPeerClosePassive(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{})
		io.WriteString(peer, "last")
		peer.Close()
		wantRecv(t, s, sock, 0, "last")
		if _, err := sock.Recv(context.Background(), s, 0); err != gentcp.ErrClosed {
			t.Errorf("Recv: %v, want closed", err)
		}
		awaitExit(t, s, sock)
	})
}

func TestAcceptCancelled(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		ls, err := gentcp.Listen(context.Background(), s, "127.0.0.1:0", gentcp.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if _, err := ls.Accept(ctx, s); err != context.DeadlineExceeded {
			t.Errorf("Accept: %v", err)
		}
		// The connection arriving after is not lost to the cancelled Accept.
		c, err := net.Dial("tcp", ls.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		time.Sleep(10 * time.Millisecond)
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		sock, err := ls.Accept(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		io.WriteString(c, "hi")
		wantRecv(t, s, sock, 2, "hi")
	})
}

func TestListenOwnerExit(t *testing.T) {
	n := proc.NewNode("")
	var addr string
	inProc(t, n, func(s *proc.Self) {
		ls, err := gentcp.Listen(context.Background(), s, "127.0.0.1:0", gentcp.Options{})
		if err != nil {
			t.Fatal(err)
		}
		addr = ls.Addr().String()
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			return
		}
		c.Close()
		if time.Now().After(deadline) {
			t.Fatal("still listening after the owner exited")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestListenCloseHeld(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		ls, err := gentcp.Listen(context.Background(), s, "127.0.0.1:0", gentcp.Options{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		ls.Accept(ctx, s)
		c, err := net.Dial("tcp", ls.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		// The connection waits for an Accept, until the socket closes.
		time.Sleep(10 * time.Millisecond)
		ls.Close(context.Background(), s)
		wantEOF(t, c)
	})
}

func TestReset(t *testing.T) {
	n := proc.NewNode("")
	reset := func(peer net.Conn) {
		peer.(*net.TCPConn).SetLinger(0)
		peer.Close()
	}
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Active: gentcp.Always})
		reset(peer)
		if m, ok := receive(t, s).(gentcp.ErrorMsg); !ok || m.Sock != sock || m.Err == nil {
			t.Errorf("got %#v, want Error", m)
		}
		if m, ok := receive(t, s).(gentcp.ClosedMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Closed", m)
		}

		sock, peer = pair(t, s, gentcp.Options{})
		reset(peer)
		_, err := sock.Recv(context.Background(), s, 0)
		if err == nil || err == gentcp.ErrClosed {
			t.Errorf("Recv: %v, want the error of the connection", err)
		}
	})
}

func TestPacketTooLargePassive(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Line, PacketSize: 4})
		io.WriteString(peer, "ok\ntoo long\n")
		wantRecv(t, s, sock, 0, "ok\n")
		if _, err := sock.Recv(context.Background(), s, 0); !errors.Is(err, gentcp.ErrPacketTooLarge) {
			t.Errorf("Recv: %v", err)
		}
		awaitExit(t, s, sock)
	})
}

// TestSendFailed fails a send on a passive socket, which does not read:
// the owner learns of it once it wants data.
func TestSendFailed(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{})
		peer.(*net.TCPConn).SetLinger(0)
		peer.Close()
		var err error
		for range 100 {
			if err = sock.Send(ctx, s, []byte("x")); err != nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if err == nil {
			t.Fatal("send never failed")
		}
		quiet(t, s)
		if e := sock.SetActive(ctx, s, gentcp.Once); e != nil {
			t.Error(e)
		}
		if m, ok := receive(t, s).(gentcp.ErrorMsg); !ok || m.Err != err {
			t.Errorf("got %#v, want Error %v", m, err)
		}
		if _, ok := receive(t, s).(gentcp.ClosedMsg); !ok {
			t.Error("no Closed")
		}
		awaitExit(t, s, sock)
	})
}

func TestStart(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		a, b := net.Pipe()
		defer b.Close()
		sock := gentcp.Start(n, a, s.PID(), gentcp.Options{Active: gentcp.Once})
		go io.WriteString(b, "hi")
		wantData(t, s, sock, "hi")
	})
}

// TestReadsOnDemand checks that a passive socket reads only for Recv, so a
// peer cannot flood it: over a pipe, a write blocks until it is read.
func TestReadsOnDemand(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		server, peer := net.Pipe()
		defer peer.Close()
		sock := gentcp.Start(n, server, s.PID(), gentcp.Options{})
		written := make(chan string, 2)
		go func() {
			for _, w := range []string{"one", "two"} {
				peer.Write([]byte(w))
				written <- w
			}
		}()
		notWritten := func() {
			t.Helper()
			select {
			case w := <-written:
				t.Errorf("%q written without a Recv", w)
			case <-time.After(50 * time.Millisecond):
			}
		}
		notWritten()
		wantRecv(t, s, sock, 0, "one")
		<-written
		notWritten()
		wantRecv(t, s, sock, 0, "two")
	})
}

// TestFailedHalfClosed: a failed connection closes, even with HalfClosed.
func TestFailedHalfClosed(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		opts := gentcp.Options{Packet: gentcp.Line, PacketSize: 2, HalfClosed: true, Active: gentcp.Once}
		sock, peer := pair(t, s, opts)
		io.WriteString(peer, "too long\n")
		if _, ok := receive(t, s).(gentcp.ErrorMsg); !ok {
			t.Error("no Error")
		}
		if _, ok := receive(t, s).(gentcp.ClosedMsg); !ok {
			t.Error("no Closed")
		}
		awaitExit(t, s, sock)
	})
}

// The tests below have a read in progress, that a Raw active socket may
// deliver to its owner without going through its process, when the owner
// changes something: nothing read after the change is delivered as before.

func TestPassiveDuringRead(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Active: gentcp.Once})
		time.Sleep(10 * time.Millisecond) // the socket is reading
		sock.SetActive(ctx, s, gentcp.Passive)
		io.WriteString(peer, "x")
		quiet(t, s)
		wantRecv(t, s, sock, 0, "x")
	})
}

func TestControlDuringRead(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Active: gentcp.Once})
		got := make(chan any, 1)
		heir := n.Spawn(func(h *proc.Self) error {
			got <- receive(t, h)
			return nil
		})
		time.Sleep(10 * time.Millisecond)
		if err := sock.ControllingProcess(ctx, s, heir); err != nil {
			t.Error(err)
		}
		io.WriteString(peer, "x")
		select {
		case m := <-got:
			if d, ok := m.(gentcp.DataMsg); !ok || string(d.Bytes) != "x" {
				t.Errorf("heir got %#v", m)
			}
		case <-time.After(2 * time.Second):
			t.Error("nothing for the heir")
		}
		quiet(t, s)
	})
}

func TestCloseDuringRead(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Active: gentcp.Once})
		time.Sleep(10 * time.Millisecond)
		sock.Close(context.Background(), s)
		io.WriteString(peer, "x")
		quiet(t, s)
	})
}

// TestRawCount delivers reads as packets of their own, and the Passive
// message after the last.
func TestRawCount(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Active: gentcp.N(2)})
		for _, w := range []string{"a", "b"} {
			io.WriteString(peer, w)
			wantData(t, s, sock, w)
		}
		if m, ok := receive(t, s).(gentcp.PassiveMsg); !ok || m.Sock != sock {
			t.Errorf("got %#v, want Passive", m)
		}
		io.WriteString(peer, "c")
		quiet(t, s)
		wantRecv(t, s, sock, 0, "c")
	})
}
