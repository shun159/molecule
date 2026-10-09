package genudp

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Socket is a UDP socket: the process owning it, and its address. It is
// plain data; its methods call the process.
type Socket struct {
	PID       proc.PID
	LocalAddr netip.AddrPort

	conn *net.UDPConn // nil in a Socket made by hand: sending goes through PID
}

// Messages a socket sends its owner.
type (
	// DataMsg is a datagram received from From, in an active mode.
	DataMsg struct {
		Sock  Socket
		From  netip.AddrPort
		Bytes []byte
	}
	// ErrorMsg tells that receiving failed, in an active mode, before
	// ClosedMsg.
	ErrorMsg struct {
		Sock Socket
		Err  error
	}
	// ClosedMsg tells that the socket closed, its reading having failed.
	ClosedMsg struct{ Sock Socket }
	// PassiveMsg tells that the socket turned passive, its N datagrams
	// sent.
	PassiveMsg struct{ Sock Socket }
	// SendErrorMsg tells a behaviour that a datagram of its SendEffect to
	// To could not be sent.
	SendErrorMsg struct {
		Sock Socket
		To   netip.AddrPort
		Err  error
	}
)

// Errors of socket calls.
var (
	// ErrClosed: the socket is closed.
	ErrClosed = errors.New("genudp: closed")
	// ErrNotOwner: only the owner may receive and give the socket away.
	ErrNotOwner = errors.New("genudp: not the owner")
	// ErrActive: Recv on a socket in an active mode.
	ErrActive = errors.New("genudp: socket is active")
	// ErrTimeout: Recv waited past the deadline of its context.
	ErrTimeout = errors.New("genudp: timeout")
)

// Requests to a socket process.
type (
	sendReq struct {
		to     netip.AddrPort
		data   []byte
		then   bool // set active after
		active Active
	}
	recvReq      struct{ timeout time.Duration } // 0: none
	setActiveReq struct{ active Active }
	controlReq   struct{ owner proc.PID }
	closeReq     struct{}
)

// recvRep is the reply to a Recv.
type recvRep struct {
	from netip.AddrPort
	data []byte
	err  error
}

// Send sends data as one datagram to to. It writes in the calling process,
// and returns once the datagram is handed to the kernel; a failure leaves
// the socket as it was.
func (s Socket) Send(ctx context.Context, caller molecule.Caller, to netip.AddrPort, data []byte) error {
	if s.conn == nil {
		return s.call(ctx, caller, sendReq{to: to, data: data})
	}
	return write(s.conn, to, data)
}

// Recv takes the next datagram of a passive socket, and where it came
// from, waiting for it. The wait ends at the deadline of ctx, if any, with
// ErrTimeout; the socket keeps whatever arrives after. Only the owner may
// receive.
func (s Socket) Recv(ctx context.Context, caller molecule.Caller) (netip.AddrPort, []byte, error) {
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		if timeout = time.Until(deadline); timeout <= 0 {
			return netip.AddrPort{}, nil, ErrTimeout
		}
	}
	// The socket ends the wait: giving up here could lose a datagram it
	// has just taken for this call.
	v, err := molecule.Call(context.Background(), caller, s.PID, recvReq{timeout})
	if err != nil {
		return netip.AddrPort{}, nil, gone(err)
	}
	switch r := v.(type) {
	case recvRep:
		return r.from, r.data, r.err
	case error:
		return netip.AddrPort{}, nil, r
	}
	return netip.AddrPort{}, nil, nil
}

// SetActive changes the active mode.
func (s Socket) SetActive(ctx context.Context, caller molecule.Caller, a Active) error {
	return s.call(ctx, caller, setActiveReq{a})
}

// ControllingProcess gives the socket to owner, which gets its messages
// from then on, like gen_udp:controlling_process. Only the owner may.
// Messages already sent stay with the previous owner.
func (s Socket) ControllingProcess(ctx context.Context, caller molecule.Caller, owner proc.PID) error {
	return s.call(ctx, caller, controlReq{owner})
}

// Close closes the socket, and its process exits.
func (s Socket) Close(ctx context.Context, caller molecule.Caller) error {
	err := s.call(ctx, caller, closeReq{})
	if err == ErrClosed {
		return nil // closed already
	}
	return err
}

// SendEffect is the effect sending data as one datagram to to, for a
// behaviour: the runtime of the behaviour writes, as Send does. A failure
// comes back to the behaviour as a SendErrorMsg. Through a Socket made by
// hand, it goes through the socket process, and a failure is not told.
func (s Socket) SendEffect(to netip.AddrPort, data []byte) molecule.Effect {
	if s.conn == nil {
		return molecule.Send{To: s.PID, Msg: sendReq{to: to, data: data}}
	}
	return sendEffect{sock: s, to: to, data: data}
}

// SendActiveEffect is the effect sending data, then changing the active
// mode, as SendEffect and SetActiveEffect do. It is how a behaviour
// answers a datagram and asks for the next:
//
//	case genudp.DataMsg:
//		return s, molecule.Do(m.Sock.SendActiveEffect(m.From, reply, genudp.Once))
//
// The mode changes even if sending fails.
func (s Socket) SendActiveEffect(to netip.AddrPort, data []byte, a Active) molecule.Effect {
	if s.conn == nil {
		return molecule.Send{To: s.PID, Msg: sendReq{to: to, data: data, then: true, active: a}}
	}
	return sendEffect{sock: s, to: to, data: data, then: true, active: a}
}

// SetActiveEffect is the effect changing the active mode, for a behaviour.
func (s Socket) SetActiveEffect(a Active) molecule.Effect {
	return molecule.Send{To: s.PID, Msg: setActiveReq{a}}
}

// CloseEffect is the effect closing the socket, for a behaviour.
func (s Socket) CloseEffect() molecule.Effect {
	return molecule.Send{To: s.PID, Msg: closeReq{}}
}

func (s Socket) call(ctx context.Context, caller molecule.Caller, req any) error {
	v, err := molecule.Call(ctx, caller, s.PID, req)
	if err != nil {
		return gone(err)
	}
	err, _ = v.(error)
	return err
}

// gone turns the error of a call to a socket whose process is gone into
// ErrClosed.
func gone(err error) error {
	var exit *molecule.ExitError
	if errors.As(err, &exit) {
		return ErrClosed
	}
	return err
}

// write sends one datagram. UDP writes are whole and safe from several
// goroutines at once, so it needs no lock.
func write(conn *net.UDPConn, to netip.AddrPort, data []byte) error {
	_, err := conn.WriteToUDPAddrPort(data, to)
	if errors.Is(err, net.ErrClosed) {
		return ErrClosed
	}
	return err
}

// sendEffect sends from the process of a behaviour, as Socket.Send does,
// then changes the active mode if then.
type sendEffect struct {
	molecule.Extension
	sock   Socket
	to     netip.AddrPort
	data   []byte
	then   bool
	active Active
}

func (e sendEffect) Perform(env gen.Env) {
	if err := write(e.sock.conn, e.to, e.data); err != nil {
		env.Send(env.Self(), SendErrorMsg{e.sock, e.to, err})
	}
	if e.then {
		env.Send(e.sock.PID, setActiveReq{e.active})
	}
}

// Open opens a socket bound to addr, owned by owner, like gen_udp:open. An
// IPv6 address may carry a zone, as "[fe80::1%eth0]:546" does.
func Open(ctx context.Context, owner *proc.Self, addr string, opts Options) (Socket, error) {
	var lc net.ListenConfig
	pc, err := lc.ListenPacket(ctx, "udp", addr)
	if err != nil {
		return Socket{}, err
	}
	return Start(owner.Node(), pc.(*net.UDPConn), owner.PID(), opts), nil
}

// Start makes a socket of conn, opened by other means, owned by owner. The
// socket owns conn from then on, and closes it.
func Start(n *proc.Node, conn *net.UDPConn, owner proc.PID, opts Options) Socket {
	sock := Socket{conn: conn}
	if a, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		sock.LocalAddr = a.AddrPort()
	}
	started := make(chan Socket)
	sock.PID = n.Spawn(func(s *proc.Self) error {
		return serve(s, <-started, owner, opts)
	})
	started <- sock
	return sock
}
