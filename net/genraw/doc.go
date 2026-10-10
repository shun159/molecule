//go:build unix

// Package genraw provides raw sockets owned by processes: any datagram
// socket a program opens itself, as socket(2) does -- an AF_PACKET socket,
// a raw ICMPv6 one, or another the net package has no type for -- read and
// written as datagrams with their addresses, like genudp does for UDP.
//
// # Opening
//
// [Open] opens a socket of a domain, type and protocol, and has setup
// prepare it before anything is read: socket options, a filter, a bind,
// whatever its protocol needs, with the syscall or golang.org/x/sys/unix
// calls the program chooses. genraw knows none of them. The process calling
// Open becomes the owner of the socket: it receives the datagrams, and the
// socket closes when it exits. [Start] makes a socket of an fd opened by
// other means.
//
//	sock, err := genraw.Open(self, syscall.AF_INET6, syscall.SOCK_RAW, syscall.IPPROTO_ICMPV6,
//		func(fd int) error { return syscall.BindToDevice(fd, "eth0") }, genraw.Options{})
//
// Addresses are syscall.Sockaddr values: a *syscall.SockaddrInet6 for a raw
// IPv6 socket, a *syscall.SockaddrLinklayer for an AF_PACKET one. A received
// one is the sender as recvfrom(2) reports it.
//
// # Receiving, sending, behaviours
//
// As in genudp: the Active option says how datagrams reach the owner --
// Passive, taken with Recv; Once, N(n) and Always, as DataMsg messages --
// and the socket reads only while the owner wants a datagram, the others
// waiting in the kernel. PacketSize bounds a datagram received, a larger
// one dropped. A failed read closes the socket, the owner told when it
// next wants a datagram. Sending writes in the process sending; a failure
// leaves the socket as it was, and through SendEffect comes back to the
// behaviour as a SendErrorMsg.
//
// The socket is read through the runtime's poller, its fd made
// non-blocking: closing it, as the end of its process does, wakes a read
// waiting, which close(2) on a blocking fd would not.
package genraw
