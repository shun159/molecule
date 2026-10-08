package gentcpacceptor

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
)

// DefaultAcceptors is the number of acceptors when Spec.Acceptors is zero.
const DefaultAcceptors = 10

// Spec describes a listener.
type Spec struct {
	// Addr is the address to listen on, as for net.Listen.
	Addr string
	// Acceptors is the number of processes accepting connections.
	Acceptors int
	// MaxConns, if positive, caps the connections: those beyond it are
	// closed as soon as accepted.
	MaxConns int
	// ActiveN is how many packets may be ahead of the handler, like
	// {active, N} in Erlang: more keeps data flowing while the handler
	// works, fewer bounds the memory a slow handler holds. Zero means
	// DefaultActiveN.
	ActiveN int
	// Options are the options of the sockets of a Behaviour, but Active,
	// which ActiveN replaces. Raw handlers have none.
	Options gentcp.Options
}

// DefaultActiveN is Spec.ActiveN when zero.
const DefaultActiveN = 4

// maxActive bounds Spec.ActiveN.
const maxActive = 1024

// Listener runs a Behaviour on the connections to an address: a
// supervisor of the listening socket, the acceptors, and the processes of
// the connections.
//
//	listener (RestForOne)
//	├── socket     the net.Listener, closed when this process dies
//	├── conns      dynamic supervisor of the handlers
//	└── acceptors  one-for-one supervisor of the acceptors
//
// If the socket fails, everything after it is restarted.
//
// A Listener runs on its own with Start, or under a supervisor of the
// application through ChildSpec.
type Listener struct {
	spec      Spec
	startConn startConn
	shared    *shared
}

// startConn starts the process of a connection, linked to parent, and
// gives it conn.
type startConn func(ctx context.Context, parent *proc.Self, conn net.Conn) (proc.PID, error)

// shared is what the children started by one listener supervisor hand to
// the later ones: the socket to the acceptors, the conns supervisor to the
// acceptors.
type shared struct {
	mu    sync.Mutex
	sup   proc.PID
	ln    net.Listener
	conns proc.PID
}

func (sh *shared) get() (net.Listener, proc.PID) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.ln, sh.conns
}

// NewListener returns a listener running b for spec, not yet started.
func NewListener[S any](spec Spec, b Behaviour[S]) *Listener {
	start := gen.StartLinkFunc(adapter[S]{b: b, activeN: activeN(spec)}, nil)
	opts := spec.Options
	opts.Active = gentcp.Passive // until Init has run
	return newListener(spec, func(ctx context.Context, parent *proc.Self, conn net.Conn) (proc.PID, error) {
		pid, err := start(ctx, parent)
		if err != nil {
			return pid, err
		}
		parent.Send(pid, attached{gentcp.Start(parent.Node(), conn, pid, opts)})
		return pid, nil
	})
}

func newListener(spec Spec, start startConn) *Listener {
	return &Listener{spec: spec, startConn: start, shared: &shared{}}
}

func activeN(spec Spec) int {
	switch {
	case spec.ActiveN <= 0:
		return DefaultActiveN
	case spec.ActiveN > maxActive:
		return maxActive
	}
	return spec.ActiveN
}

// Start starts a listener running b on its own.
func Start[S any](ctx context.Context, n *proc.Node, spec Spec, b Behaviour[S]) (*Listener, error) {
	return start(ctx, n, NewListener(spec, b))
}

func start(ctx context.Context, n *proc.Node, l *Listener) (*Listener, error) {
	sup, err := supervisor.Start(ctx, n, l.supervisorSpec())
	if err != nil {
		return nil, err
	}
	l.setPID(sup)
	return l, nil
}

// StartLink starts the listener linked to parent. It is a
// supervisor.StartFunc, as used by ChildSpec.
func (l *Listener) StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error) {
	sup, err := supervisor.StartLink(ctx, parent, l.supervisorSpec())
	if err == nil {
		l.setPID(sup)
	}
	return sup, err
}

// ChildSpec returns the spec to run the listener under a supervisor. When
// it is restarted there, the Listener follows the new processes.
func (l *Listener) ChildSpec(id string) supervisor.ChildSpec {
	return supervisor.ChildSpec{ID: id, Start: l.StartLink, Type: supervisor.Supervisor}
}

func (l *Listener) setPID(sup proc.PID) {
	l.shared.mu.Lock()
	defer l.shared.mu.Unlock()
	l.shared.sup = sup
}

// PID returns the supervisor of the listener.
func (l *Listener) PID() proc.PID {
	l.shared.mu.Lock()
	defer l.shared.mu.Unlock()
	return l.shared.sup
}

// Addr returns the address the listener listens on.
func (l *Listener) Addr() net.Addr {
	ln, _ := l.shared.get()
	return ln.Addr()
}

// Conns returns the dynamic supervisor of the connection handlers, to
// count or list them.
func (l *Listener) Conns() proc.PID {
	_, conns := l.shared.get()
	return conns
}

// Stop stops a listener started with Start: it stops accepting, and
// closes every connection by stopping its handler. A listener under a
// supervisor is stopped by that supervisor instead.
func (l *Listener) Stop(ctx context.Context, caller molecule.Caller) error {
	return supervisor.Stop(ctx, caller, l.PID())
}

func (l *Listener) supervisorSpec() supervisor.Spec {
	spec, sh := l.spec, l.shared
	acceptors := spec.Acceptors
	if acceptors <= 0 {
		acceptors = DefaultAcceptors
	}
	var accs []supervisor.ChildSpec
	for i := range acceptors {
		accs = append(accs, supervisor.ChildSpec{
			ID: "acceptor" + strconv.Itoa(i),
			Start: func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
				return parent.StartLink(ctx, func(s *proc.Self) error {
					s.InitAck(nil)
					return accept(s, sh, l.startConn)
				})
			},
		})
	}

	return supervisor.Spec{
		Strategy: supervisor.RestForOne,
		Children: []supervisor.ChildSpec{
			{ID: "socket", Start: func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
				return parent.StartLink(ctx, func(s *proc.Self) error { return listen(s, spec.Addr, sh) })
			}},
			{ID: "conns", Type: supervisor.Supervisor, Start: func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
				pid, err := supervisor.StartDynamicLink(ctx, parent, supervisor.DynamicSpec{MaxChildren: spec.MaxConns})
				if err == nil {
					sh.mu.Lock()
					sh.conns = pid
					sh.mu.Unlock()
				}
				return pid, err
			}},
			{ID: "acceptors", Type: supervisor.Supervisor, Start: supervisor.StartLinkFunc(supervisor.Spec{Children: accs})},
		},
	}
}

// listen owns the listening socket, closed when the process dies.
func listen(s *proc.Self, addr string, sh *shared) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		s.InitAck(err)
		return err
	}
	// Asked to stop, the process closes the socket before it exits, so
	// that once it is seen dead, no connection is accepted anymore. Killed,
	// it runs no code: the socket is closed a little after, by AfterFunc.
	defer ln.Close()
	context.AfterFunc(s.Context(), func() { ln.Close() })
	s.TrapExit(true)
	sh.mu.Lock()
	sh.ln = ln
	sh.mu.Unlock()
	s.InitAck(nil)
	for {
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		if e, ok := msg.(proc.ExitMsg); ok {
			return e.Reason
		}
	}
}

// accept accepts connections and starts a handler for each under the conns
// supervisor.
func accept(s *proc.Self, sh *shared, startConn startConn) error {
	ln, conns := sh.get()
	for {
		conn, err := ln.Accept()
		if reason := s.ExitReason(); reason != nil {
			// Killed while blocked in Accept, which only the closing of
			// the socket ends.
			if conn != nil {
				conn.Close()
			}
			return reason
		}
		if errors.Is(err, net.ErrClosed) {
			// The socket is gone, and with it the listener supervisor
			// is about to stop or restart us: wait for that rather than
			// fail in a loop.
			_, err := s.Receive(context.Background())
			return err
		}
		if err != nil {
			continue
		}
		_, err = supervisor.StartChild(context.Background(), s, conns, supervisor.ChildSpec{
			Restart: supervisor.Temporary,
			Start: func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
				pid, err := startConn(ctx, parent, conn)
				if err != nil {
					conn.Close()
				}
				return pid, err
			},
		})
		if err != nil {
			conn.Close() // over MaxConns, or the handler failed to start
		}
	}
}
