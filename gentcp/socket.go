package gentcp

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// Socket is a connected socket: the process owning the connection, and
// its addresses. It is plain data; its methods call the process.
type Socket struct {
	PID        proc.PID
	LocalAddr  net.Addr
	RemoteAddr net.Addr

	w *writer // nil in a Socket made by hand: sending goes through PID
}

// Messages a socket sends its owner.
type (
	// DataMsg is a packet received, in an active mode.
	DataMsg struct {
		Sock  Socket
		Bytes []byte
	}
	// ClosedMsg tells that the peer closed the connection, or that it
	// failed, in an active mode. With HalfClosed, the socket may still
	// send; otherwise it is gone.
	ClosedMsg struct{ Sock Socket }
	// ErrorMsg tells that receiving failed, before ClosedMsg.
	ErrorMsg struct {
		Sock Socket
		Err  error
	}
	// PassiveMsg tells that the socket turned passive, its N packets sent.
	PassiveMsg struct{ Sock Socket }
)

// Errors of socket calls.
var (
	// ErrClosed: the connection is closed, for receiving at least.
	ErrClosed = errors.New("gentcp: closed")
	// ErrNotOwner: only the owner may receive and give the socket away.
	ErrNotOwner = errors.New("gentcp: not the owner")
	// ErrActive: Recv on a socket in an active mode.
	ErrActive = errors.New("gentcp: socket is active")
	// ErrTimeout: Recv waited past the deadline of its context.
	ErrTimeout = errors.New("gentcp: timeout")
)

// How says which side of the connection Shutdown closes.
type How int

const (
	Read How = iota
	Write
	ReadWrite
)

// Requests to a socket process.
type (
	sendReq struct {
		data   []byte
		then   bool // set active after
		active Active
	}
	recvReq struct {
		length  int
		timeout time.Duration // 0: none
	}
	setActiveReq struct{ active Active }
	controlReq   struct{ owner proc.PID }
	shutdownReq  struct{ how How }
	closeReq     struct{}
)

// Send sends data, framed as the Packet option says. It writes in the
// calling process, as gen_tcp_socket does, and returns once the data is
// written, or fails after SendTimeout. A failure of the connection fails
// the socket, as for receiving.
func (s Socket) Send(ctx context.Context, caller gen.Caller, data []byte) error {
	if s.w == nil {
		return s.call(ctx, caller, sendReq{data: data})
	}
	failed, err := s.w.write(data)
	if failed {
		caller.Node().Send(s.PID, sendFailed{err})
	}
	return err
}

// Recv takes the next packet of a passive socket, waiting for it. For
// Raw, length 0 takes what there is, and a positive length exactly that
// many bytes. The wait ends at the deadline of ctx, if any, with
// ErrTimeout; the socket keeps whatever arrives after. Only the owner may
// receive.
func (s Socket) Recv(ctx context.Context, caller gen.Caller, length int) ([]byte, error) {
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		if timeout = time.Until(deadline); timeout <= 0 {
			return nil, ErrTimeout
		}
	}
	// The socket ends the wait: giving up here could lose a packet it has
	// just taken for this call.
	v, err := gen.Call(context.Background(), caller, s.PID, recvReq{length, timeout})
	if err != nil {
		return nil, gone(err)
	}
	switch r := v.(type) {
	case []byte:
		return r, nil
	case error:
		return nil, r
	}
	return nil, nil
}

// SetActive changes the active mode.
func (s Socket) SetActive(ctx context.Context, caller gen.Caller, a Active) error {
	return s.call(ctx, caller, setActiveReq{a})
}

// ControllingProcess gives the socket to owner, which gets its messages
// from then on, like gen_tcp:controlling_process. Only the owner may.
// Messages already sent stay with the previous owner.
func (s Socket) ControllingProcess(ctx context.Context, caller gen.Caller, owner proc.PID) error {
	return s.call(ctx, caller, controlReq{owner})
}

// Shutdown closes one side of the connection, or both, like
// gen_tcp:shutdown: Write tells the peer no more data comes, while the
// socket still receives. It needs a TCP connection, and fails on another,
// such as TLS, given to Start.
func (s Socket) Shutdown(ctx context.Context, caller gen.Caller, how How) error {
	return s.call(ctx, caller, shutdownReq{how})
}

// Close closes the socket, and its process exits.
func (s Socket) Close(ctx context.Context, caller gen.Caller) error {
	err := s.call(ctx, caller, closeReq{})
	if err == ErrClosed {
		return nil // closed already
	}
	return err
}

// SendEffect is the effect sending data, for a behaviour: the runtime of
// the behaviour writes, as Send does. A failure breaks the connection,
// which the owner is told of as it is of a failure to receive.
func (s Socket) SendEffect(data []byte) gen.Effect {
	if s.w == nil {
		return gen.Send{To: s.PID, Msg: sendReq{data: data}}
	}
	return sendEffect{pid: s.PID, w: s.w, data: data}
}

// SendActiveEffect is the effect sending data, then changing the active
// mode, as SendEffect and SetActiveEffect do, in one message to the
// socket rather than two. It is how a behaviour answers a packet and asks
// for the next:
//
//	case gentcp.DataMsg:
//		return s, gen.Do(m.Sock.SendActiveEffect(reply, gentcp.Once))
//
// The mode changes even if sending fails, so that the owner hears of the
// failure.
func (s Socket) SendActiveEffect(data []byte, a Active) gen.Effect {
	if s.w == nil {
		return gen.Send{To: s.PID, Msg: sendReq{data: data, then: true, active: a}}
	}
	return sendEffect{pid: s.PID, w: s.w, data: data, then: true, active: a}
}

// SetActiveEffect is the effect changing the active mode, for a behaviour.
func (s Socket) SetActiveEffect(a Active) gen.Effect {
	return gen.Send{To: s.PID, Msg: setActiveReq{a}}
}

// CloseEffect is the effect closing the socket, for a behaviour.
func (s Socket) CloseEffect() gen.Effect {
	return gen.Send{To: s.PID, Msg: closeReq{}}
}

func (s Socket) call(ctx context.Context, caller gen.Caller, req any) error {
	v, err := gen.Call(ctx, caller, s.PID, req)
	if err != nil {
		return gone(err)
	}
	err, _ = v.(error)
	return err
}

// gone turns the error of a call to a socket whose process is gone into
// ErrClosed.
func gone(err error) error {
	var exit *gen.ExitError
	if errors.As(err, &exit) {
		return ErrClosed
	}
	return err
}

// Connect connects to addr and returns a socket owned by owner, like
// gen_tcp:connect.
func Connect(ctx context.Context, owner *proc.Self, addr string, opts Options) (Socket, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return Socket{}, err
	}
	return Start(owner.Node(), conn, owner.PID(), opts), nil
}

// Start makes a socket of conn, a connection made by other means, owned by
// owner. The socket owns conn from then on, and closes it.
func Start(n *proc.Node, conn net.Conn, owner proc.PID, opts Options) Socket {
	w := newWriter(conn, opts)
	addrs := Socket{LocalAddr: conn.LocalAddr(), RemoteAddr: conn.RemoteAddr(), w: w}
	started := make(chan struct{})
	sock := addrs
	sock.PID = n.Spawn(func(s *proc.Self) error {
		<-started
		sock := addrs
		sock.PID = s.PID()
		return serve(s, conn, sock, owner, opts)
	})
	// The process exists now: its end is the end of sending.
	ctx, _ := n.Watch(context.Background(), sock.PID)
	w.done = ctx.Done()
	close(started)
	return sock
}
