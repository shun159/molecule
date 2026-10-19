package gentcpacceptor

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"sync"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/net/gentcp"
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
	// closed as soon as accepted. The cap is shared out among the
	// supervisors of the connections; a connection goes to another when
	// that of its acceptor is full.
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
//	├── conns      one-for-one supervisor of the supervisors of the handlers
//	│   └── conns0…  a dynamic supervisor for each acceptor
//	└── acceptors  one-for-one supervisor of the acceptors
//
// If the socket fails, everything after it is restarted. Each acceptor
// starts its handlers under a supervisor of its own, as Ranch's
// num_conns_sups does, so that acceptors do not wait on one another.
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
// the later ones: the socket and the supervisors of the connections to
// the acceptors.
type shared struct {
	mu    sync.Mutex
	sup   proc.PID
	ln    net.Listener
	conns []proc.PID // as many as shards, set as each is started
}

func (sh *shared) listener() net.Listener {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.ln
}

// connsAt returns the supervisor of the connections of shard i, the one
// started last.
func (sh *shared) connsAt(i int) proc.PID {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return sh.conns[i]
}

// NewListener returns a listener running b for spec, not yet started.
func NewListener[S any](spec Spec, b Behaviour[S]) *Listener {
	start := gen.ChildOf(adapter[S]{b: b, activeN: activeN(spec)}, nil)
	opts := spec.Options
	opts.Active = gentcp.Passive // until Init has run
	return newListener(spec, func(ctx context.Context, parent *proc.Self, conn net.Conn) (proc.PID, error) {
		pid, err := start.StartLink(ctx, parent)
		if err != nil {
			return pid, err
		}
		parent.Send(pid, attached{gentcp.Start(parent.Node(), conn, pid, opts)})
		return pid, nil
	})
}

func newListener(spec Spec, start startConn) *Listener {
	l := &Listener{spec: spec, startConn: start, shared: &shared{}}
	l.shared.conns = make([]proc.PID, l.shards())
	return l
}

// numAcceptors is Spec.Acceptors, DefaultAcceptors if not set.
func (l *Listener) numAcceptors() int {
	if l.spec.Acceptors <= 0 {
		return DefaultAcceptors
	}
	return l.spec.Acceptors
}

// shards is the number of supervisors of the connections: one for each
// acceptor, but no more than MaxConns, for each to take one at least.
func (l *Listener) shards() int {
	n := l.numAcceptors()
	if m := l.spec.MaxConns; m > 0 && m < n {
		return m
	}
	return n
}

// shardMax is the MaxChildren of the supervisor of shard i: MaxConns
// shared out, the remainder to the first ones.
func shardMax(maxConns, shards, i int) int {
	if maxConns <= 0 {
		return 0
	}
	m := maxConns / shards
	if i < maxConns%shards {
		m++
	}
	return m
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
	return supervisor.ChildSpec{ID: id, Start: supervisor.StartFunc(l.StartLink), Type: supervisor.Supervisor}
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
	return l.shared.listener().Addr()
}

// Conns returns the dynamic supervisors of the connection handlers, one
// for each acceptor, to count or list them.
func (l *Listener) Conns() []proc.PID {
	l.shared.mu.Lock()
	defer l.shared.mu.Unlock()
	return slices.Clone(l.shared.conns)
}

// Stop stops a listener started with Start: it stops accepting, and
// closes every connection by stopping its handler. A listener under a
// supervisor is stopped by that supervisor instead.
func (l *Listener) Stop(ctx context.Context, caller actor.Caller) error {
	return supervisor.Stop(ctx, caller, l.PID())
}

// supervisorSpec is the tree of the listener: the socket, the supervisor of
// the connections, and the acceptors, restarted after the socket if it
// fails.
func (l *Listener) supervisorSpec() supervisor.Spec {
	return supervisor.Spec{
		Strategy: supervisor.RestForOne,
		Children: []supervisor.ChildSpec{
			{ID: "socket", Start: l.socketStart()},
			{ID: "conns", Type: supervisor.Supervisor, Start: supervisor.Child(supervisor.Spec{Children: l.connsSups()})},
			{ID: "acceptors", Type: supervisor.Supervisor, Start: supervisor.Child(supervisor.Spec{Children: l.acceptors()})},
		},
	}
}

// socketStart starts the process of the listening socket.
func (l *Listener) socketStart() supervisor.StartFunc {
	addr, sh := l.spec.Addr, l.shared
	return func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return parent.StartLink(ctx, func(s *proc.Self) error { return listen(s, addr, sh) })
	}
}

// connsSups are the children of the supervisor of the connections, one
// for each shard.
func (l *Listener) connsSups() []supervisor.ChildSpec {
	n := l.shards()
	sups := make([]supervisor.ChildSpec, 0, n)
	for i := range n {
		sups = append(sups, supervisor.ChildSpec{ID: "conns" + strconv.Itoa(i), Type: supervisor.Supervisor, Start: l.connsStart(i)})
	}
	return sups
}

// connsStart starts the supervisor of the connections of shard i, and
// shares its PID with the acceptors.
func (l *Listener) connsStart(i int) supervisor.StartFunc {
	maxChildren, sh := shardMax(l.spec.MaxConns, l.shards(), i), l.shared
	return func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		pid, err := supervisor.StartDynamicLink(ctx, parent, supervisor.DynamicSpec{MaxChildren: maxChildren})
		if err == nil {
			sh.mu.Lock()
			sh.conns[i] = pid
			sh.mu.Unlock()
		}
		return pid, err
	}
}

// acceptors are the children of the supervisor of the acceptors, as many
// as Acceptors, DefaultAcceptors if not set.
func (l *Listener) acceptors() []supervisor.ChildSpec {
	n := l.numAcceptors()
	accs := make([]supervisor.ChildSpec, 0, n)
	for i := range n {
		accs = append(accs, supervisor.ChildSpec{ID: "acceptor" + strconv.Itoa(i), Start: l.acceptorStart(i % l.shards())})
	}
	return accs
}

// acceptorStart starts an acceptor, starting its handlers under the
// supervisor of shard first.
func (l *Listener) acceptorStart(first int) supervisor.StartFunc {
	sh := l.shared
	return func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return parent.StartLink(ctx, func(s *proc.Self) error {
			s.SetLabel("gentcpacceptor acceptor")
			s.InitAck(nil)
			return accept(s, sh, first, l.startConn)
		})
	}
}

// listen owns the listening socket, closed when the process dies.
func listen(s *proc.Self, addr string, sh *shared) error {
	s.SetLabel("gentcpacceptor socket " + addr)
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

// accept accepts connections and starts a handler for each under a
// supervisor of the connections, that of shard first if it can.
func accept(s *proc.Self, sh *shared, first int, startConn startConn) error {
	ln := sh.listener()
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
		if err == nil {
			startHandler(s, sh, first, conn, startConn)
		}
	}
}

// startHandler starts the handler of conn under the supervisor of shard
// first, or of the next ones in turn while they are full. It closes conn
// if it cannot: all full, over MaxConns, or the handler failed to start.
func startHandler(s *proc.Self, sh *shared, first int, conn net.Conn, startConn startConn) {
	spec := supervisor.ChildSpec{
		Restart: supervisor.Temporary,
		Start: supervisor.StartFunc(func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
			pid, err := startConn(ctx, parent, conn)
			if err != nil {
				conn.Close()
			}
			return pid, err
		}),
	}
	shards := len(sh.conns)
	for k := range shards {
		conns := sh.connsAt((first + k) % shards)
		_, err := supervisor.StartChild(context.Background(), s, conns, spec)
		if !errors.Is(err, supervisor.ErrMaxChildren) {
			if err != nil {
				conn.Close()
			}
			return
		}
	}
	conn.Close()
}
