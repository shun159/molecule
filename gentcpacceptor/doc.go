// Package gentcpacceptor is a behaviour for TCP servers, like a Ranch
// protocol: a pool of acceptors hands each connection to a process of its
// own, which runs the callbacks of a [Behaviour].
//
// A connection is a gentcp socket, a process of its own, as a port is in
// Erlang. The packets read from it arrive as arguments of the callbacks,
// and sending is an effect, so the callbacks stay pure.
//
// A connection may instead be handled by a [Handler], plain Go code
// reading and writing the connection itself, as a Ranch protocol does with
// its transport; see Raw handlers.
//
// # Listeners
//
// A [Listener] serves one address. It is a supervision tree:
//
//	listener (rest_for_one)
//	├── socket     the listening socket, closed when this process dies
//	├── conns      dynamic supervisor of the connection processes
//	└── acceptors  supervisor of the acceptors
//
// If the socket fails, the connections and acceptors are restarted with
// it. A Listener runs on its own with [Start], or under a supervisor of
// the program with [Listener.ChildSpec].
//
// The fields of [Spec]:
//
//	Addr       the address, as for net.Listen
//	Acceptors  processes accepting connections; DefaultAcceptors if zero
//	MaxConns   connections allowed at a time; no limit if zero
//	ActiveN    packets allowed ahead of the handler; DefaultActiveN if zero
//	Options    the gentcp options of the sockets: framing, PacketSize,
//	           HalfClosed, SendTimeout
//
// A connection accepted beyond MaxConns is closed at once.
//
// # Callbacks
//
//	Init(self, gentcp.Socket) (S, []gen.Effect, error)         when the connection is ready
//	HandleData(S, gentcp.Socket, []byte) (S, []gen.Effect)     on a packet
//	HandleClosed(S, gentcp.Socket, error) (S, []gen.Effect)    on the connection closing, optional
//	HandleInfo(S, gentcp.Socket, any) (S, []gen.Effect)        on other messages, optional
//	Terminate(S, error) []gen.Effect                           when stopping, optional
//
// The optional callbacks are those of [ClosedHandler], [InfoHandler] and
// [Terminator]. HandleData gets a packet as the Packet option cuts them: a
// line, a length-prefixed message, or with gentcp.Raw what one read
// returned, where a message of the protocol may come in pieces, or several
// at once. The data belongs to the handler, which may keep it.
//
// The socket is a gentcp.Socket, owned by the process of the handler.
// Its SendEffect and CloseEffect make the effects sending to and closing
// the connection.
//
// # Flow control
//
// The socket reads only as far as allowed, like {active, N}: at most
// ActiveN packets are delivered and not yet handled, and each HandleData
// allows one more. A handler slower than its peer keeps the data in the
// kernel rather than in its mailbox.
//
// # Closing
//
// When the peer closes the connection, the handler stops normally. When
// reading or sending fails, or a packet is too large, it stops with the
// error wrapped in proc.Shutdown, which is not reported: a lost connection
// is no failure of the handler. HandleClosed replaces this. Whichever way
// the handler process ends, the connection is closed.
//
// With the HalfClosed option, a connection closed by the peer stays open
// for sending: HandleClosed gets a nil error and may still answer, like
// gen_tcp with {exit_on_close, false}.
//
// # Raw handlers
//
// [StartRaw] and [NewRawListener] run a [Handler] on each connection: a
// function owning the net.Conn, in the process of the connection. The
// listener, the acceptors, MaxConns, supervision, reports and Stop are the
// same; the data goes from the connection to the handler with nothing in
// between, at the speed of plain Go. A Behaviour costs four messages a
// round trip, but is pure, tested by calling its functions, reads on
// demand, and gets whole packets; a Handler suits connections whose data
// needs little handling, such as proxies and streams. The Options do not
// apply to it: it owns the net.Conn, and does as it likes.
package gentcpacceptor
