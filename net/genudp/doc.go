// Package genudp provides UDP sockets owned by processes, like gen_udp of
// Erlang: each socket is a process, which reads datagrams and hands them to
// the process owning it.
//
// # Sockets and owners
//
// [Open] opens a socket bound to an address; the process calling it
// becomes the owner of the socket, the controlling process of gen_udp: it
// receives the datagrams, and the socket closes when it exits.
// [Socket.ControllingProcess] gives a socket to another process. [Start]
// makes a socket of a connection opened by other means, for socket options
// Open does not set.
//
//	sock, err := genudp.Open(ctx, self, "127.0.0.1:5353", genudp.Options{})
//	from, pkt, err := sock.Recv(ctx, self)
//	err = sock.Send(ctx, self, from, pkt)
//
// A [Socket] is plain data, the PID of its process and its address; any
// process may send through it, and only the owner receive. Sending writes
// in the process sending, rather than going through the socket process; a
// datagram is written whole or not at all.
//
// # Receiving
//
// The Active option says how the datagrams reach the owner:
//
//	Passive   the owner takes them with Recv
//	Once      the next arrives as a DataMsg, then the socket is passive
//	N(n)      the next n arrive as DataMsg, then a PassiveMsg
//	Always    every one arrives as a DataMsg
//
// [Socket.SetActive] changes the mode. The socket reads only when the owner
// wants a datagram, so in the first three modes the mailbox of the owner
// stays bounded: datagrams wait in the kernel, which drops them when its
// buffer is full, as it does for any UDP socket not read. Always gives that
// up.
//
// Recv waits until the deadline of its context, if any, and then returns
// [ErrTimeout]. A datagram arriving after stays in the socket for the next
// Recv.
//
// PacketSize bounds the size of a datagram received; a larger one is
// dropped. The default bound is the largest UDP payload.
//
// A socket whose reading fails closes: in an active mode, the owner gets an
// [ErrorMsg] then a [ClosedMsg]; in passive mode, Recv returns the error.
// Either comes when the owner wants a datagram, after the datagrams
// received before.
//
// # Sending
//
// [Socket.Send] sends a datagram to an address. A failure, such as a
// datagram too large, is returned and leaves the socket as it was: UDP has
// no connection to break.
//
// # Behaviours
//
// The messages of a socket arrive to a behaviour as info messages, and
// [Socket.SendEffect], [Socket.SetActiveEffect] and [Socket.CloseEffect]
// act on it as effects, so that the behaviour stays pure. The runtime of
// the behaviour performs a SendEffect itself, see gen.Performer; a send
// that fails comes back to the behaviour as a [SendErrorMsg].
// [Socket.SendActiveEffect] answers a datagram and asks for the next in one
// message:
//
//	case genudp.DataMsg:
//		return s, molecule.Do(m.Sock.SendActiveEffect(m.From, reply, genudp.Once))
package genudp
