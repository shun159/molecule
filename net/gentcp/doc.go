// Package gentcp provides TCP sockets owned by processes, like gen_tcp of
// Erlang in its gen_tcp_socket implementation: each socket is a process,
// which reads from the connection, cuts the bytes into packets, and hands
// them to the process owning it.
//
// # Sockets and owners
//
// [Connect] connects to an address, [Listen] listens on one, and
// [ListenSocket.Accept] accepts a connection. The process calling them
// becomes the owner of the socket, the controlling process of gen_tcp: it
// receives the data, and the socket closes when it exits.
// [Socket.ControllingProcess] gives a socket to another process.
//
//	ls, err := gentcp.Listen(ctx, self, ":5555", gentcp.Options{Packet: gentcp.Packet4})
//	sock, err := ls.Accept(ctx, self)
//	pkt, err := sock.Recv(ctx, self, 0)
//	err = sock.Send(ctx, self, pkt)
//
// A [Socket] is plain data, the PID of its process and its addresses; any
// process may send through it, and only the owner receive. Sending writes
// in the process sending, as gen_tcp_socket does, rather than going
// through the socket process; writes are kept whole and in order. A
// ListenSocket is shared by the processes accepting on it.
//
// # Receiving
//
// The Active option says how the data reaches the owner:
//
//	Passive   the owner takes packets with Recv
//	Once      the next packet arrives as a DataMsg, then the socket is passive
//	N(n)      the next n packets arrive as DataMsg, then a PassiveMsg
//	Always    every packet arrives as a DataMsg
//
// [Socket.SetActive] changes the mode. The socket reads from the
// connection only when the owner wants data, so in the first three modes
// a peer sending faster than the owner handles is held back by TCP, and
// the mailbox of the owner stays bounded. Always gives that up.
//
// In an active mode, the end of the connection arrives as a [ClosedMsg],
// after an [ErrorMsg] if the connection failed; in passive mode, Recv
// returns [ErrClosed] or the error. Either comes when the owner wants
// data, after the packets received before.
//
// Recv waits until the deadline of its context, if any, and then returns
// [ErrTimeout]. Data arriving after stays in the socket for the next Recv.
//
// # Packets
//
// The Packet option says how the bytes are cut:
//
//	Raw                        whatever was read; Recv may ask for a length
//	Packet1, Packet2, Packet4  a big-endian length prefix, added by Send
//	Line                       up to a newline, kept in the packet
//
// PacketSize bounds a packet received. A packet beyond it fails the
// connection; bytes left that make no whole packet when the connection
// ends are dropped.
//
// # Closing
//
// [Socket.Close] closes the socket, and so does the exit of its owner. A
// connection that failed, in receiving or in sending, closes once the
// owner is told.
// When the peer closes its side, the socket closes too, unless HalfClosed
// is set: then it can still send, like gen_tcp with {exit_on_close, false}.
// [Socket.Shutdown] closes one side: Write tells the peer that nothing
// more is sent, while the socket still receives.
//
// # Behaviours
//
// The messages of a socket arrive to a behaviour as info messages, and
// [Socket.SendEffect], [Socket.SetActiveEffect] and [Socket.CloseEffect]
// act on it as effects, so that the behaviour stays pure. The runtime of
// the behaviour performs a SendEffect itself, see gen.Performer.
// [Socket.SendActiveEffect] answers a packet and asks for the next in one
// message. A failed
// SendEffect fails the connection, which the owner learns as above.
// A socket is made by a process with Connect or Accept, then given to the
// behaviour with ControllingProcess.
package gentcp
