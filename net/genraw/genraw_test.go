//go:build unix

package genraw_test

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/net/genraw"
	"github.com/shun159/molecule/proc"
)

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

// open opens a UDP socket on the loopback the raw way, owned by s, and a
// plain peer; it returns the socket's address.
func open(t *testing.T, s *proc.Self, opts genraw.Options) (genraw.Socket, *syscall.SockaddrInet4, *net.UDPConn) {
	t.Helper()
	var addr *syscall.SockaddrInet4
	sock, err := genraw.Open(s, syscall.AF_INET, syscall.SOCK_DGRAM, 0, func(fd int) error {
		if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
			return err
		}
		sa, err := syscall.Getsockname(fd)
		addr = sa.(*syscall.SockaddrInet4)
		return err
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	return sock, addr, peer
}

func peerAddr(peer *net.UDPConn) *syscall.SockaddrInet4 {
	return &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}, Port: peer.LocalAddr().(*net.UDPAddr).Port}
}

func send(t *testing.T, peer *net.UDPConn, to *syscall.SockaddrInet4, data string) {
	t.Helper()
	if _, err := peer.WriteToUDP([]byte(data), &net.UDPAddr{IP: to.Addr[:], Port: to.Port}); err != nil {
		t.Fatal(err)
	}
}

func receive(t *testing.T, s *proc.Self) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	msg, err := s.Receive(ctx)
	if err != nil {
		t.Fatal("no message:", err)
	}
	return msg
}

func awaitExit(t *testing.T, s *proc.Self, sock genraw.Socket) {
	t.Helper()
	ctx, cancel := s.Node().Watch(context.Background(), sock.PID)
	defer cancel()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Error("socket process still alive")
	}
}

func TestRecvAndSend(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, addr, peer := open(t, s, genraw.Options{})
		send(t, peer, addr, "ping")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		from, data, err := sock.Recv(ctx, s)
		if err != nil || string(data) != "ping" {
			t.Fatalf("Recv = %v %q %v", from, data, err)
		}
		if sa, ok := from.(*syscall.SockaddrInet4); !ok || sa.Port != peerAddr(peer).Port {
			t.Fatalf("from %#v", from)
		}
		if err := sock.Send(context.Background(), s, from, []byte("pong")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 16)
		peer.SetReadDeadline(time.Now().Add(2 * time.Second))
		if k, err := peer.Read(buf); err != nil || string(buf[:k]) != "pong" {
			t.Errorf("peer got %q, %v", buf[:k], err)
		}
	})
}

func TestActiveN(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, addr, peer := open(t, s, genraw.Options{Active: genraw.N(2)})
		for _, d := range []string{"a", "b"} {
			send(t, peer, addr, d)
		}
		for _, want := range []string{"a", "b"} {
			if m, ok := receive(t, s).(genraw.DataMsg); !ok || string(m.Bytes) != want {
				t.Fatalf("got %#v, want DataMsg %q", m, want)
			}
		}
		if m, ok := receive(t, s).(genraw.PassiveMsg); !ok || m.Sock.PID != sock.PID {
			t.Fatalf("got %#v, want PassiveMsg", m)
		}
	})
}

func TestSetupFails(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		boom := errors.New("boom")
		if _, err := genraw.Open(s, syscall.AF_INET, syscall.SOCK_DGRAM, 0, func(int) error { return boom }, genraw.Options{}); err != boom {
			t.Errorf("Open = %v, want the setup's error", err)
		}
	})
}

// Closing a socket wakes its reader, waiting for a datagram that never
// comes: the socket process ends at once.
func TestCloseWakesReader(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, _, _ := open(t, s, genraw.Options{Active: genraw.Always})
		time.Sleep(20 * time.Millisecond) // the reader waits
		if err := sock.Close(context.Background(), s); err != nil {
			t.Fatal(err)
		}
		awaitExit(t, s, sock)
		if err := sock.Send(context.Background(), s, &syscall.SockaddrInet4{Port: 9}, []byte("x")); err != genraw.ErrClosed {
			t.Errorf("Send after Close = %v, want ErrClosed", err)
		}
	})
}

// echo answers each datagram of the socket cast to it with the same, one
// at a time, and passes a SendErrorMsg on to the process to.
type echo struct {
	genserver.Default[struct{}]
	to proc.PID
}

type badSend struct{ sock genraw.Socket }

func (echo) HandleCast(s struct{}, msg any) (struct{}, []molecule.Effect) {
	switch m := msg.(type) {
	case genraw.Socket:
		return s, molecule.Do(m.SetActiveEffect(genraw.Once))
	case badSend:
		// An IPv6 address on an IPv4 socket.
		return s, molecule.Do(m.sock.SendEffect(&syscall.SockaddrInet6{Port: 9}, []byte("x")))
	}
	return s, nil
}

func (e echo) HandleInfo(s struct{}, msg any) (struct{}, []molecule.Effect) {
	switch m := msg.(type) {
	case genraw.DataMsg:
		return s, molecule.Do(m.Sock.SendActiveEffect(m.From, m.Bytes, genraw.Once))
	case genraw.SendErrorMsg:
		return s, molecule.Do(molecule.Send{To: e.to, Msg: m})
	}
	return s, nil
}

func TestEffects(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		srv, err := genserver.Start(ctx, n, echo{to: s.PID()})
		if err != nil {
			t.Fatal(err)
		}
		pid, _ := srv.Dest().WhereIs(n)
		sock, addr, peer := open(t, s, genraw.Options{})
		if err := sock.ControllingProcess(ctx, s, pid); err != nil {
			t.Fatal(err)
		}
		srv.Cast(s, sock)
		buf := make([]byte, 16)
		for _, d := range []string{"one", "two"} {
			send(t, peer, addr, d)
			peer.SetReadDeadline(time.Now().Add(2 * time.Second))
			if k, err := peer.Read(buf); err != nil || string(buf[:k]) != d {
				t.Fatalf("peer got %q, %v", buf[:k], err)
			}
		}
		srv.Cast(s, badSend{sock})
		if m, ok := receive(t, s).(genraw.SendErrorMsg); !ok || m.Err == nil {
			t.Fatalf("got %#v, want SendErrorMsg", m)
		}
	})
}
