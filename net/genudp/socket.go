package genudp

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/net/internal/dgram"
	"github.com/shun159/molecule/proc"
)

// Socket is a UDP socket: the process owning it, and its address. It is
// plain data; its methods call the process.
type Socket struct {
	PID       proc.PID
	LocalAddr netip.AddrPort

	conn dgram.Conn[netip.AddrPort] // nil in a Socket made by hand: sending goes through PID
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

// kind is what genudp makes of the datagram socket core.
var kind = &dgram.Kind[netip.AddrPort, Socket]{
	Name:      "genudp",
	Data:      func(s Socket, from netip.AddrPort, b []byte) any { return DataMsg{s, from, b} },
	Error:     func(s Socket, err error) any { return ErrorMsg{s, err} },
	Closed:    func(s Socket) any { return ClosedMsg{s} },
	Passive:   func(s Socket) any { return PassiveMsg{s} },
	SendError: func(s Socket, to netip.AddrPort, err error) any { return SendErrorMsg{s, to, err} },

	ErrClosed:   ErrClosed,
	ErrNotOwner: ErrNotOwner,
	ErrActive:   ErrActive,
	ErrTimeout:  ErrTimeout,
}

// Send sends data as one datagram to to. It writes in the calling process,
// and returns once the datagram is handed to the kernel; a failure leaves
// the socket as it was.
func (s Socket) Send(ctx context.Context, caller molecule.Caller, to netip.AddrPort, data []byte) error {
	if s.conn == nil {
		return dgram.Call(ctx, kind, caller, s.PID, dgram.SendReq[netip.AddrPort]{To: to, Data: data})
	}
	return dgram.Write(kind, s.conn, to, data)
}

// Recv takes the next datagram of a passive socket, and where it came
// from, waiting for it. The wait ends at the deadline of ctx, if any, with
// ErrTimeout; the socket keeps whatever arrives after. Only the owner may
// receive.
func (s Socket) Recv(ctx context.Context, caller molecule.Caller) (netip.AddrPort, []byte, error) {
	return dgram.Recv(ctx, kind, caller, s.PID)
}

// SetActive changes the active mode.
func (s Socket) SetActive(ctx context.Context, caller molecule.Caller, a Active) error {
	return dgram.Call(ctx, kind, caller, s.PID, dgram.SetActiveReq{Active: a})
}

// ControllingProcess gives the socket to owner, which gets its messages
// from then on, like gen_udp:controlling_process. Only the owner may.
// Messages already sent stay with the previous owner.
func (s Socket) ControllingProcess(ctx context.Context, caller molecule.Caller, owner proc.PID) error {
	return dgram.Call(ctx, kind, caller, s.PID, dgram.ControlReq{Owner: owner})
}

// Close closes the socket, and its process exits.
func (s Socket) Close(ctx context.Context, caller molecule.Caller) error {
	err := dgram.Call(ctx, kind, caller, s.PID, dgram.CloseReq{})
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
	return dgram.SendEffect(kind, s, s.PID, s.conn, to, data, false, Passive)
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
	return dgram.SendEffect(kind, s, s.PID, s.conn, to, data, true, a)
}

// SetActiveEffect is the effect changing the active mode, for a behaviour.
func (s Socket) SetActiveEffect(a Active) molecule.Effect {
	return molecule.Send{To: s.PID, Msg: dgram.SetActiveReq{Active: a}}
}

// CloseEffect is the effect closing the socket, for a behaviour.
func (s Socket) CloseEffect() molecule.Effect {
	return molecule.Send{To: s.PID, Msg: dgram.CloseReq{}}
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
	sock := Socket{conn: udpConn{conn}}
	if a, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		sock.LocalAddr = a.AddrPort()
	}
	started := make(chan Socket)
	sock.PID = n.Spawn(func(s *proc.Self) error {
		sock := <-started
		return dgram.Serve(s, kind, sock.conn, sock, owner, opts.Active, opts.packetSize(), "socket "+sock.LocalAddr.String())
	})
	started <- sock
	return sock
}

// udpConn is a *net.UDPConn as the core reads it.
type udpConn struct{ c *net.UDPConn }

// ReadFrom reports an IPv4 sender on a dual-stack socket as IPv4, not as
// an IPv4-mapped IPv6 address.
func (u udpConn) ReadFrom(buf []byte) (int, netip.AddrPort, error) {
	n, from, err := u.c.ReadFromUDPAddrPort(buf)
	return n, netip.AddrPortFrom(from.Addr().Unmap(), from.Port()), err
}

func (u udpConn) WriteTo(b []byte, to netip.AddrPort) error {
	_, err := u.c.WriteToUDPAddrPort(b, to)
	return err
}

func (u udpConn) Close() error { return u.c.Close() }
