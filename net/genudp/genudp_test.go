package genudp_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/shun159/molecule/net/genudp"
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

// open opens a socket owned by s and a plain peer, both on the loopback.
func open(t *testing.T, s *proc.Self, opts genudp.Options) (genudp.Socket, *net.UDPConn) {
	t.Helper()
	sock, err := genudp.Open(context.Background(), s, "127.0.0.1:0", opts)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.Close() })
	return sock, peer
}

func peerAddr(peer *net.UDPConn) netip.AddrPort {
	return peer.LocalAddr().(*net.UDPAddr).AddrPort()
}

func send(t *testing.T, peer *net.UDPConn, sock genudp.Socket, data string) {
	t.Helper()
	if _, err := peer.WriteToUDPAddrPort([]byte(data), sock.LocalAddr); err != nil {
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

func quiet(t *testing.T, s *proc.Self) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if msg, err := s.Receive(ctx); err == nil {
		t.Errorf("unexpected %#v", msg)
	}
}

func wantData(t *testing.T, s *proc.Self, from netip.AddrPort, want string) {
	t.Helper()
	m, ok := receive(t, s).(genudp.DataMsg)
	if !ok || string(m.Bytes) != want || m.From != from {
		t.Fatalf("got %#v, want DataMsg %q from %v", m, want, from)
	}
}

func wantRecv(t *testing.T, s *proc.Self, sock genudp.Socket, from netip.AddrPort, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, data, err := sock.Recv(ctx, s)
	if err != nil || string(data) != want || got != from {
		t.Fatalf("Recv = %v %q %v, want %v %q", got, data, err, from, want)
	}
}

func awaitExit(t *testing.T, s *proc.Self, sock genudp.Socket) {
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
		sock, peer := open(t, s, genudp.Options{})
		send(t, peer, sock, "ping")
		wantRecv(t, s, sock, peerAddr(peer), "ping")
		if err := sock.Send(context.Background(), s, peerAddr(peer), []byte("pong")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 16)
		peer.SetReadDeadline(time.Now().Add(2 * time.Second))
		k, from, err := peer.ReadFromUDPAddrPort(buf)
		if err != nil || string(buf[:k]) != "pong" || from != sock.LocalAddr {
			t.Errorf("peer got %q from %v, %v", buf[:k], from, err)
		}
	})
}

// A datagram arriving after a Recv timed out stays for the next.
func TestRecvTimeout(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, _, err := sock.Recv(ctx, s); err != genudp.ErrTimeout {
			t.Fatalf("Recv = %v, want ErrTimeout", err)
		}
		send(t, peer, sock, "late")
		time.Sleep(50 * time.Millisecond)
		wantRecv(t, s, sock, peerAddr(peer), "late")
	})
}

func TestActiveOnce(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{Active: genudp.Once})
		send(t, peer, sock, "one")
		send(t, peer, sock, "two")
		wantData(t, s, peerAddr(peer), "one")
		quiet(t, s)
		if err := sock.SetActive(context.Background(), s, genudp.Once); err != nil {
			t.Fatal(err)
		}
		wantData(t, s, peerAddr(peer), "two")
	})
}

func TestActiveN(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{Active: genudp.N(2)})
		for _, d := range []string{"a", "b", "c"} {
			send(t, peer, sock, d)
		}
		wantData(t, s, peerAddr(peer), "a")
		wantData(t, s, peerAddr(peer), "b")
		if m, ok := receive(t, s).(genudp.PassiveMsg); !ok || m.Sock.PID != sock.PID {
			t.Fatalf("got %#v, want PassiveMsg", m)
		}
		quiet(t, s)
		wantRecv(t, s, sock, peerAddr(peer), "c")
	})
}

func TestActiveAlways(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{Active: genudp.Always})
		for i := range 50 {
			send(t, peer, sock, fmt.Sprint(i))
		}
		for i := range 50 {
			wantData(t, s, peerAddr(peer), fmt.Sprint(i))
		}
	})
}

func TestRecvWhileActive(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, _ := open(t, s, genudp.Options{Active: genudp.Always})
		if _, _, err := sock.Recv(context.Background(), s); err != genudp.ErrActive {
			t.Errorf("Recv = %v, want ErrActive", err)
		}
	})
}

func TestPacketSize(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{PacketSize: 4})
		send(t, peer, sock, "too large")
		send(t, peer, sock, "fits")
		wantRecv(t, s, sock, peerAddr(peer), "fits")
	})
}

func TestOwnerExit(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		socks := make(chan genudp.Socket, 1)
		n.Spawn(func(o *proc.Self) error {
			sock, err := genudp.Open(context.Background(), o, "127.0.0.1:0", genudp.Options{})
			if err != nil {
				return err
			}
			socks <- sock
			return nil
		})
		sock := <-socks
		awaitExit(t, s, sock)
		if err := sock.Send(context.Background(), s, sock.LocalAddr, []byte("x")); err != genudp.ErrClosed {
			t.Errorf("Send = %v, want ErrClosed", err)
		}
	})
}

func TestControllingProcess(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{Active: genudp.Once})
		got := make(chan genudp.DataMsg, 1)
		ready := make(chan struct{})
		other := n.Spawn(func(o *proc.Self) error {
			if err := sock.ControllingProcess(context.Background(), o, o.PID()); err != genudp.ErrNotOwner {
				t.Errorf("ControllingProcess by another = %v, want ErrNotOwner", err)
			}
			close(ready)
			msg, err := o.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg.(genudp.DataMsg)
			return nil
		})
		<-ready
		if err := sock.ControllingProcess(context.Background(), s, other); err != nil {
			t.Fatal(err)
		}
		send(t, peer, sock, "yours")
		select {
		case m := <-got:
			if string(m.Bytes) != "yours" {
				t.Errorf("new owner got %q", m.Bytes)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("new owner got nothing")
		}
		quiet(t, s)
	})
}

func TestClose(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, _ := open(t, s, genudp.Options{})
		if err := sock.Close(context.Background(), s); err != nil {
			t.Fatal(err)
		}
		awaitExit(t, s, sock)
		if err := sock.Close(context.Background(), s); err != nil {
			t.Errorf("second Close = %v", err)
		}
		if err := sock.Send(context.Background(), s, sock.LocalAddr, []byte("x")); err != genudp.ErrClosed {
			t.Errorf("Send = %v, want ErrClosed", err)
		}
	})
}

// A Socket made by hand, its PID alone, sends through the socket process.
func TestByHand(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{})
		byHand := genudp.Socket{PID: sock.PID}
		if err := byHand.Send(context.Background(), s, peerAddr(peer), []byte("via process")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 32)
		peer.SetReadDeadline(time.Now().Add(2 * time.Second))
		if k, _, err := peer.ReadFromUDPAddrPort(buf); err != nil || string(buf[:k]) != "via process" {
			t.Errorf("peer got %q, %v", buf[:k], err)
		}
	})
}

func TestSendTooLarge(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, peer := open(t, s, genudp.Options{})
		err := sock.Send(context.Background(), s, peerAddr(peer), make([]byte, 70000))
		if err == nil || errors.Is(err, genudp.ErrClosed) {
			t.Fatalf("Send = %v, want a failure", err)
		}
		// The socket is as it was.
		send(t, peer, sock, "still open")
		wantRecv(t, s, sock, peerAddr(peer), "still open")
	})
}

// On a dual-stack socket, an IPv4 sender is reported as IPv4, not as an
// IPv4-mapped IPv6 address.
func TestDualStackUnmapped(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, err := genudp.Open(context.Background(), s, "[::]:0", genudp.Options{})
		if err != nil {
			t.Skip("no IPv6:", err)
		}
		peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		to := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), sock.LocalAddr.Port())
		if _, err := peer.WriteToUDPAddrPort([]byte("v4"), to); err != nil {
			t.Fatal(err)
		}
		wantRecv(t, s, sock, peerAddr(peer), "v4")
	})
}
