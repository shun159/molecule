//go:build unix

package socket

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/net/internal/dgram"
	"github.com/shun159/molecule/proc"
)

// maxPacketSize is the default bound on a datagram received.
const maxPacketSize = 65535

// Options configure a socket. The zero value is a passive socket receiving
// datagrams of any size.
type Options struct {
	// Active is how datagrams reach the owner: taken with Recv, or sent as
	// DataMsg messages.
	Active Active
	// PacketSize, if positive, bounds the size of a datagram received; a
	// larger one is dropped.
	PacketSize int
}

func (o Options) packetSize() int {
	if o.PacketSize <= 0 || o.PacketSize > maxPacketSize {
		return maxPacketSize
	}
	return o.PacketSize
}

// Active is the active mode of a socket, as in genudp.
type Active = dgram.Active

var (
	// Passive leaves datagrams in the socket until Recv takes them. It is
	// the default.
	Passive = dgram.Passive
	// Once sends the next datagram as a DataMsg, then turns passive.
	Once = dgram.Once
	// Always sends every datagram as a DataMsg. A slow owner sees its
	// mailbox grow.
	Always = dgram.Always
)

// N sends the next n datagrams as DataMsg messages, then a PassiveMsg,
// turning passive. Setting N on a socket already in this mode adds n to
// what is left.
func N(n int) Active { return dgram.N(n) }

// Socket is a raw socket: the process owning it. It is plain data; its
// methods call the process.
type Socket struct {
	PID proc.PID

	conn dgram.Conn[syscall.Sockaddr] // nil in a Socket made by hand: sending goes through PID
}

// Messages a socket sends its owner.
type (
	// DataMsg is a datagram received from From, in an active mode.
	DataMsg struct {
		Sock  Socket
		From  syscall.Sockaddr
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
		To   syscall.Sockaddr
		Err  error
	}
)

// Errors of socket calls.
var (
	// ErrClosed: the socket is closed.
	ErrClosed = errors.New("socket: closed")
	// ErrNotOwner: only the owner may receive and give the socket away.
	ErrNotOwner = errors.New("socket: not the owner")
	// ErrActive: Recv on a socket in an active mode.
	ErrActive = errors.New("socket: socket is active")
	// ErrTimeout: Recv waited past the deadline of its context.
	ErrTimeout = errors.New("socket: timeout")
)

// kind is what socket makes of the datagram socket core.
var kind = &dgram.Kind[syscall.Sockaddr, Socket]{
	Name:      "socket",
	Data:      func(s Socket, from syscall.Sockaddr, b []byte) any { return DataMsg{s, from, b} },
	Error:     func(s Socket, err error) any { return ErrorMsg{s, err} },
	Closed:    func(s Socket) any { return ClosedMsg{s} },
	Passive:   func(s Socket) any { return PassiveMsg{s} },
	SendError: func(s Socket, to syscall.Sockaddr, err error) any { return SendErrorMsg{s, to, err} },

	ErrClosed:   ErrClosed,
	ErrNotOwner: ErrNotOwner,
	ErrActive:   ErrActive,
	ErrTimeout:  ErrTimeout,
}

// Send sends data as one datagram to to. It writes in the calling process;
// a failure leaves the socket as it was.
func (s Socket) Send(ctx context.Context, caller molecule.Caller, to syscall.Sockaddr, data []byte) error {
	if s.conn == nil {
		return dgram.Call(ctx, kind, caller, s.PID, dgram.SendReq[syscall.Sockaddr]{To: to, Data: data})
	}
	return dgram.Write(kind, s.conn, to, data)
}

// Recv takes the next datagram of a passive socket, and where it came
// from, waiting for it. The wait ends at the deadline of ctx, if any, with
// ErrTimeout; the socket keeps whatever arrives after. Only the owner may
// receive.
func (s Socket) Recv(ctx context.Context, caller molecule.Caller) (syscall.Sockaddr, []byte, error) {
	return dgram.Recv(ctx, kind, caller, s.PID)
}

// SetActive changes the active mode.
func (s Socket) SetActive(ctx context.Context, caller molecule.Caller, a Active) error {
	return dgram.Call(ctx, kind, caller, s.PID, dgram.SetActiveReq{Active: a})
}

// ControllingProcess gives the socket to owner, which gets its messages
// from then on. Only the owner may. Messages already sent stay with the
// previous owner.
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
func (s Socket) SendEffect(to syscall.Sockaddr, data []byte) molecule.Effect {
	return dgram.SendEffect(kind, s, s.PID, s.conn, to, data, false, Passive)
}

// SendActiveEffect is the effect sending data, then changing the active
// mode, as SendEffect and SetActiveEffect do: how a behaviour answers a
// datagram and asks for the next. The mode changes even if sending fails.
func (s Socket) SendActiveEffect(to syscall.Sockaddr, data []byte, a Active) molecule.Effect {
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

// Open opens a socket of domain, typ and proto, as socket(2) does, owned
// by owner. setup, if not nil, prepares the socket's fd before anything is
// read from it -- socket options, a filter, a bind -- and must not keep it.
// If socket(2) or setup fails, the fd is closed and the error returned.
func Open(owner *proc.Self, domain, typ, proto int, setup func(fd int) error, opts Options) (Socket, error) {
	fd, err := syscall.Socket(domain, typ, proto)
	if err != nil {
		return Socket{}, fmt.Errorf("socket: socket: %w", err)
	}
	syscall.CloseOnExec(fd)
	if setup != nil {
		if err := setup(fd); err != nil {
			syscall.Close(fd)
			return Socket{}, err
		}
	}
	return Start(owner.Node(), fd, owner.PID(), opts)
}

// Start makes a socket of fd, opened by other means, owned by owner. The
// socket owns fd from then on, made non-blocking, and closes it; if Start
// fails, fd is closed.
func Start(n *proc.Node, fd int, owner proc.PID, opts Options) (Socket, error) {
	conn, err := newConn(fd)
	if err != nil {
		return Socket{}, err
	}
	sock := Socket{conn: conn}
	label := fmt.Sprintf("fd %d", fd)
	started := make(chan Socket)
	sock.PID = n.Spawn(func(s *proc.Self) error {
		sock := <-started
		return dgram.Serve(s, kind, sock.conn, sock, owner, opts.Active, opts.packetSize(), label)
	})
	started <- sock
	return sock, nil
}

// rawConn is a socket read and written through the runtime's poller.
type rawConn struct {
	f  *os.File
	rc syscall.RawConn
}

func newConn(fd int) (rawConn, error) {
	if err := syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return rawConn{}, fmt.Errorf("socket: making the socket non-blocking: %w", err)
	}
	f := os.NewFile(uintptr(fd), "socket")
	rc, err := f.SyscallConn()
	if err != nil {
		f.Close()
		return rawConn{}, err
	}
	return rawConn{f: f, rc: rc}, nil
}

func (c rawConn) ReadFrom(buf []byte) (n int, from syscall.Sockaddr, err error) {
	rerr := c.rc.Read(func(fd uintptr) bool {
		n, from, err = syscall.Recvfrom(int(fd), buf, 0)
		return err != syscall.EAGAIN
	})
	if rerr != nil {
		return 0, nil, os.ErrClosed // RawConn.Read fails only on the fd closed
	}
	return n, from, err
}

func (c rawConn) WriteTo(b []byte, to syscall.Sockaddr) error {
	var err error
	werr := c.rc.Write(func(fd uintptr) bool {
		err = syscall.Sendto(int(fd), b, 0, to)
		return err != syscall.EAGAIN
	})
	if werr != nil {
		return os.ErrClosed // RawConn.Write fails only on the fd closed
	}
	return err
}

func (c rawConn) Close() error { return c.f.Close() }
