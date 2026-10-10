//go:build linux

package socket_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/shun159/molecule/net/socket"
	"github.com/shun159/molecule/proc"
)

// A raw ICMPv6 socket, as Router Advertisement and Neighbor Discovery use:
// an echo request to the loopback draws the kernel's echo reply. It needs
// CAP_NET_RAW, and is skipped without.
func TestICMPv6Echo(t *testing.T) {
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		sock, err := socket.Open(s, syscall.AF_INET6, syscall.SOCK_RAW, syscall.IPPROTO_ICMPV6, func(fd int) error {
			// Only echo replies (129) pass the filter.
			var filt syscall.ICMPv6Filter
			for i := range filt.Data {
				filt.Data[i] = 0xffffffff
			}
			filt.Data[129/32] &^= 1 << (129 % 32)
			return syscall.SetsockoptICMPv6Filter(fd, syscall.IPPROTO_ICMPV6, syscall.ICMPV6_FILTER, &filt)
		}, socket.Options{})
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skip("no CAP_NET_RAW:", err)
		}
		if err != nil {
			t.Fatal(err)
		}
		loopback := &syscall.SockaddrInet6{Addr: [16]byte{15: 1}}
		// Type 128, code 0, checksum (filled in by the kernel), id, seq.
		request := []byte{128, 0, 0, 0, 0x12, 0x34, 0, 1, 'h', 'i'}
		if err := sock.Send(context.Background(), s, loopback, request); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		from, reply, err := sock.Recv(ctx, s)
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) < 2 || reply[0] != 129 || string(reply[8:]) != "hi" {
			t.Fatalf("reply %v", reply)
		}
		if sa, ok := from.(*syscall.SockaddrInet6); !ok || sa.Addr != loopback.Addr {
			t.Errorf("from %#v", from)
		}
	})
}

// An AF_PACKET socket, as DHCPv4 and NDProxy servers use: bound to the
// loopback, it sees the IPv4 packet of a UDP datagram sent over it, the
// sender a link-layer address. It needs CAP_NET_RAW, and is skipped without.
func TestPacketSocket(t *testing.T) {
	lo, err := net.InterfaceByName("lo")
	if err != nil {
		t.Skip("no lo:", err)
	}
	n := proc.NewNode("")
	inProc(t, n, func(s *proc.Self) {
		proto := int(htons(syscall.ETH_P_IP))
		sock, err := socket.Open(s, syscall.AF_PACKET, syscall.SOCK_DGRAM, proto, func(fd int) error {
			return syscall.Bind(fd, &syscall.SockaddrLinklayer{Protocol: htons(syscall.ETH_P_IP), Ifindex: lo.Index})
		}, socket.Options{Active: socket.Always})
		if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			t.Skip("no CAP_NET_RAW:", err)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer sock.Close(context.Background(), s)
		c, err := net.Dial("udp4", "127.0.0.1:9")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		marker := "molecule packet socket"
		c.Write([]byte(marker))
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			m, ok := receive(t, s).(socket.DataMsg)
			if !ok || !strings.Contains(string(m.Bytes), marker) {
				continue // other traffic on lo
			}
			if ll, ok := m.From.(*syscall.SockaddrLinklayer); !ok || ll.Ifindex != lo.Index {
				t.Errorf("from %#v", m.From)
			}
			if m.Bytes[0]>>4 != 4 {
				t.Errorf("not an IPv4 packet: %x", m.Bytes[:1])
			}
			return
		}
		t.Fatal("the datagram's packet was not seen")
	})
}

func htons(v uint16) uint16 { return v<<8 | v>>8 }
