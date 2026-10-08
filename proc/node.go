package proc

import (
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

// DefaultNodeName is used when NewNode is given an empty name.
const DefaultNodeName = "nonode@nohost"

// Node is a runtime instance that owns a set of local processes, the
// counterpart of an Erlang node. Several nodes may live in one OS process,
// which is handy for testing distribution.
type Node struct {
	name     string
	creation uint32
	nextID   atomic.Uint64
	nextRef  atomic.Uint64
	procs    procTable
	aliases  aliasTable

	dist       atomic.Pointer[Distribution] // nil until Distribute
	remoteMons remoteMonTable
	nodes      nodeTable

	spawned, exited, crashed atomic.Uint64

	regMu sync.Mutex
	names map[string]PID // guarded by regMu, as is process.name

	logger *slog.Logger // nil: slog.Default()
}

// NewNode creates a node. Each node gets a fresh, non-zero creation so
// PIDs from a previous node with the same name are never mistaken as alive.
func NewNode(name string, opts ...Option) *Node {
	if name == "" {
		name = DefaultNodeName
	}
	n := &Node{name: name, creation: rand.Uint32() | 1, names: make(map[string]PID)}
	for _, opt := range opts {
		opt(n)
	}
	return n
}

// Name returns the node name.
func (n *Node) Name() string { return n.name }

// Node returns n itself, so a Node can stand in for a process (whose
// Self.Node returns its node) where only the node matters.
func (n *Node) Node() *Node { return n }

// Spawn starts fn in a new process on n and returns its PID.
func (n *Node) Spawn(fn func(*Self) error) PID {
	return n.spawn(fn).pid
}

// Send delivers msg to the mailbox of to. Like Erlang's !, it never fails:
// messages to dead, stale or unreachable processes are silently dropped.
func (n *Node) Send(to PID, msg any) {
	if p := n.lookup(to); p != nil {
		p.mbox.push(msg)
		return
	}
	if d := n.remote(to); d != nil {
		d.Send(to, msg)
	}
}

// NewPID returns a new PID of n for a process n does not run, such as one
// simulated by gensim. No process of n ever has it.
func (n *Node) NewPID() PID {
	return PID{node: n.name, creation: n.creation, id: n.nextID.Add(1)}
}

// MakeRef returns a new unique reference.
func (n *Node) MakeRef() Ref {
	return Ref{node: n.name, creation: n.creation, id: n.nextRef.Add(1)}
}

// IsAlive reports whether the local process pid is alive. It always
// returns false for remote PIDs.
func (n *Node) IsAlive(pid PID) bool {
	p := n.lookup(pid)
	return p != nil && !p.dead.Load()
}

func (n *Node) isLocal(pid PID) bool {
	return pid.node == n.name && pid.creation == n.creation
}

func (n *Node) lookup(pid PID) *process {
	if !n.isLocal(pid) {
		return nil
	}
	return n.procs.get(pid.id)
}

func (n *Node) spawn(fn func(*Self) error) *process {
	p := n.register()
	go p.run(fn)
	return p
}

// register creates a process and makes it reachable by PID without
// starting it, so links can be set up before it runs.
func (n *Node) register() *process {
	p := newProcess(n, PID{node: n.name, creation: n.creation, id: n.nextID.Add(1)})
	n.procs.put(p.pid.id, p)
	n.spawned.Add(1)
	return p
}

// sendExit delivers an exit signal from from to to.
func (n *Node) sendExit(from, to PID, reason error, viaLink bool) {
	if p := n.lookup(to); p != nil {
		p.signalExit(from, reason, viaLink)
		return
	}
	if d := n.remote(to); d != nil {
		d.Exit(from, to, reason, viaLink)
	}
}
