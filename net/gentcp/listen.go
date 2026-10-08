package gentcp

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/proc"
)

// ListenSocket is a listening socket, owned by a process: it closes when
// its owner exits, or on Close.
type ListenSocket struct {
	PID  proc.PID
	ln   net.Listener
	opts Options

	closed   context.Context // done when the listening socket is closed
	accepted chan accepted

	mu        sync.Mutex
	waiting   int // Accept calls waiting
	accepting int // goroutines accepting, or holding a connection
}

type accepted struct {
	conn net.Conn
	err  error
}

// Listen listens on addr, owned by owner, like gen_tcp:listen. The sockets
// it accepts take opts.
func Listen(ctx context.Context, owner *proc.Self, addr string, opts Options) (*ListenSocket, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	ownerPID := owner.PID()
	pid, err := owner.Node().Start(ctx, func(s *proc.Self) error {
		context.AfterFunc(s.Context(), func() { ln.Close() })
		defer ln.Close()
		watch := s.Monitor(ownerPID)
		s.InitAck(nil)
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			switch m := msg.(type) {
			case molecule.CallMsg:
				if _, ok := m.Req.(closeReq); ok {
					ln.Close()
					molecule.SendReply(s, m.From, nil)
					return nil
				}
			case proc.DownMsg:
				if m.Ref == watch {
					return nil
				}
			}
		}
	})
	if err != nil {
		ln.Close()
		return nil, err
	}
	closed, _ := owner.Node().Watch(context.Background(), pid)
	return &ListenSocket{PID: pid, ln: ln, opts: opts, closed: closed, accepted: make(chan accepted)}, nil
}

// Addr returns the address listened on.
func (l *ListenSocket) Addr() net.Addr { return l.ln.Addr() }

// Accept waits for a connection and returns its socket, owned by owner,
// like gen_tcp:accept. Several processes may accept at once. The wait
// ends with ctx, or the death of owner; a connection arriving then waits
// for the next Accept.
func (l *ListenSocket) Accept(ctx context.Context, owner *proc.Self) (Socket, error) {
	l.mu.Lock()
	l.waiting++
	if l.accepting < l.waiting {
		l.accepting++
		go l.accept()
	}
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.waiting--
		l.mu.Unlock()
	}()

	select {
	case a := <-l.accepted:
		if a.err != nil {
			if errors.Is(a.err, net.ErrClosed) {
				return Socket{}, ErrClosed
			}
			return Socket{}, a.err
		}
		return Start(owner.Node(), a.conn, owner.PID(), l.opts), nil
	case <-l.closed.Done():
		return Socket{}, ErrClosed
	case <-ctx.Done():
		return Socket{}, ctx.Err()
	case <-owner.Done():
		return Socket{}, owner.ExitReason()
	}
}

// accept accepts one connection, and holds it until an Accept takes it or
// the listening socket closes.
func (l *ListenSocket) accept() {
	conn, err := l.ln.Accept()
	select {
	case l.accepted <- accepted{conn, err}:
	case <-l.closed.Done():
		if conn != nil {
			conn.Close()
		}
	}
	l.mu.Lock()
	l.accepting--
	l.mu.Unlock()
}

// Close closes the listening socket.
func (l *ListenSocket) Close(ctx context.Context, caller molecule.Caller) error {
	_, err := molecule.Call(ctx, caller, l.PID, closeReq{})
	var exit *molecule.ExitError
	if errors.As(err, &exit) {
		return nil // closed already
	}
	return err
}
