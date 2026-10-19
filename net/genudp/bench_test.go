package genudp_test

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/shun159/molecule/net/genudp"
	"github.com/shun159/molecule/proc"
)

// BenchmarkEcho sends datagrams one at a time to a socket over loopback,
// its owner sending each back: received by Recv, passive, and as data
// messages, active.
func BenchmarkEcho(b *testing.B) {
	b.Run("passive", func(b *testing.B) {
		benchEcho(b, genudp.Passive, func(s *proc.Self, sock genudp.Socket) (netip.AddrPort, []byte, error) {
			return sock.Recv(context.Background(), s)
		})
	})
	b.Run("always", func(b *testing.B) {
		benchEcho(b, genudp.Always, func(s *proc.Self, _ genudp.Socket) (netip.AddrPort, []byte, error) {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return netip.AddrPort{}, nil, err
			}
			m := msg.(genudp.DataMsg)
			return m.From, m.Bytes, nil
		})
	})
}

// benchEcho runs an owner of a socket active as given, sending back what
// recv takes, and a client sending it a datagram at a time.
func benchEcho(b *testing.B, active genudp.Active, recv func(*proc.Self, genudp.Socket) (netip.AddrPort, []byte, error)) {
	n := proc.NewNode("")
	addr := make(chan netip.AddrPort, 1)
	owner := n.Spawn(func(s *proc.Self) error {
		sock, err := genudp.Open(context.Background(), s, "127.0.0.1:0", genudp.Options{Active: active})
		if err != nil {
			return err
		}
		addr <- sock.LocalAddr
		for {
			from, data, err := recv(s, sock)
			if err != nil {
				return err
			}
			if err := sock.Send(context.Background(), s, from, data); err != nil {
				return err
			}
		}
	})
	b.Cleanup(func() { n.Spawn(func(s *proc.Self) error { s.Exit(owner, proc.Kill); return nil }) })
	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	to := <-addr
	ping, buf := make([]byte, 64), make([]byte, 64)
	for b.Loop() {
		if _, err := client.WriteToUDPAddrPort(ping, to); err != nil {
			b.Fatal(err)
		}
		client.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, err := client.ReadFromUDPAddrPort(buf); err != nil {
			b.Fatal(err)
		}
	}
}
