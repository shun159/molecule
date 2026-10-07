package gentcpacceptor

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// Socket is the connection, as a Behaviour sees it: the process owning it,
// and its addresses. It is plain data; its methods only make effects.
type Socket struct {
	PID        proc.PID
	LocalAddr  net.Addr
	RemoteAddr net.Addr
}

// Write is the effect writing b to the connection. A failed write closes
// the connection, which is then reported as closed with the error.
func (s Socket) Write(b []byte) gen.Effect { return gen.Send{To: s.PID, Msg: write{b}} }

// Close is the effect closing the connection. No closing is reported to
// the handler then: it is expected to stop as well.
func (s Socket) Close() gen.Effect { return gen.Send{To: s.PID, Msg: closeReq{}} }

// activeOnce asks the socket to read once, like {active, once} in Erlang.
func (s Socket) activeOnce() gen.Effect { return gen.Send{To: s.PID, Msg: activeOnce{}} }

// Messages from a socket process to its owner.
type (
	attached struct{ sock Socket }
	data     struct {
		sock proc.PID
		b    []byte
	}
	closed struct {
		sock proc.PID
		err  error // nil when the peer closed
	}
)

// Requests to a socket process.
type (
	activeOnce struct{}
	write      struct{ b []byte }
	closeReq   struct{}
	readFailed struct{ err error }
)

// readBufferSize is the most a single read delivers.
const readBufferSize = 32 << 10

// attach starts the socket process of conn for owner and tells owner.
func attach(parent *proc.Self, c net.Conn, owner proc.PID) {
	pid := parent.Spawn(func(s *proc.Self) error { return runSocket(s, c, owner) })
	parent.Send(owner, attached{Socket{PID: pid, LocalAddr: c.LocalAddr(), RemoteAddr: c.RemoteAddr()}})
}

// runSocket owns c for owner: it reads on demand, writes on request, and
// closes c when either itself or owner terminates, whatever the reason.
func runSocket(s *proc.Self, c net.Conn, owner proc.PID) error {
	// Closing the connection is also what unblocks a pending Read once the
	// process is dead.
	context.AfterFunc(s.Context(), func() { c.Close() })
	defer c.Close()
	ownerRef := s.Monitor(owner)

	n, self := s.Node(), s.PID()
	permits := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, readBufferSize)
		for {
			select {
			case <-permits:
			case <-s.Context().Done():
				return
			}
			k, err := c.Read(buf)
			if k > 0 {
				n.Send(owner, data{sock: self, b: append([]byte(nil), buf[:k]...)})
			}
			if err != nil {
				n.Send(self, readFailed{err})
				return
			}
		}
	}()

	for {
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case activeOnce:
			select {
			case permits <- struct{}{}:
			default: // already asked
			}
		case write:
			if _, err := c.Write(m.b); err != nil {
				s.Send(owner, closed{sock: self, err: err})
				return nil
			}
		case readFailed:
			err := m.err
			if errors.Is(err, io.EOF) {
				err = nil
			}
			s.Send(owner, closed{sock: self, err: err})
			return nil
		case closeReq:
			return nil
		case proc.DownMsg:
			if m.Ref == ownerRef {
				return nil
			}
		}
	}
}
