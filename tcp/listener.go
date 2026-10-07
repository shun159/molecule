package tcp

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
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
	// Handler starts the process handling one connection, linked to
	// parent; gen.StartLinkFunc and genserver.StartLinkFunc make one. The
	// handler then gets Attached, and owns the connection: it is closed
	// when the handler terminates.
	Handler supervisor.StartFunc
}

// Listener is a running listener: a supervisor of the listening socket,
// the acceptors, and the handlers of the connections.
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
	spec   Spec
	shared *shared
}

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

// NewListener returns a listener for spec, not yet started.
func NewListener(spec Spec) *Listener {
	return &Listener{spec: spec, shared: &shared{}}
}

// Start starts a listener on its own.
func Start(ctx context.Context, n *proc.Node, spec Spec) (*Listener, error) {
	l := NewListener(spec)
	sup, err := supervisor.Start(ctx, n, listenerSpec(spec, l.shared))
	if err != nil {
		return nil, err
	}
	l.setPID(sup)
	return l, nil
}

// StartLink starts the listener linked to parent. It is a
// supervisor.StartFunc, as used by ChildSpec.
func (l *Listener) StartLink(ctx context.Context, parent *proc.Self) (proc.PID, error) {
	sup, err := supervisor.StartLink(ctx, parent, listenerSpec(l.spec, l.shared))
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
func (l *Listener) Stop(ctx context.Context, caller gen.Caller) error {
	return supervisor.Stop(ctx, caller, l.PID())
}

func listenerSpec(spec Spec, sh *shared) supervisor.Spec {
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
					return accept(s, sh, spec.Handler)
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
	context.AfterFunc(s.Context(), func() { ln.Close() })
	sh.mu.Lock()
	sh.ln = ln
	sh.mu.Unlock()
	s.InitAck(nil)
	for {
		if _, err := s.Receive(context.Background()); err != nil {
			return err
		}
	}
}

// accept accepts connections and starts a handler for each under the conns
// supervisor.
func accept(s *proc.Self, sh *shared, handler supervisor.StartFunc) error {
	ln, conns := sh.get()
	for {
		conn, err := ln.Accept()
		if ctx := s.Context(); ctx.Err() != nil {
			// Killed while blocked in Accept, which only the closing of
			// the socket ends.
			if conn != nil {
				conn.Close()
			}
			return context.Cause(ctx)
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
				pid, err := handler(ctx, parent)
				if err != nil {
					conn.Close()
					return pid, err
				}
				attach(parent, conn, pid)
				return pid, nil
			},
		})
		if err != nil {
			conn.Close() // over MaxConns, or the handler failed to start
		}
	}
}
