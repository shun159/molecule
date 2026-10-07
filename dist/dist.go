package dist

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/shun159/molecule/proc"
)

// Config configures the distribution of a node.
type Config struct {
	// Listen is the address to accept connections from other nodes on, as
	// for net.Listen. Empty, the node only connects to others.
	Listen string
	// Cookie is the secret the nodes share: a node with another cookie is
	// refused.
	Cookie string
	// Resolve returns the address of a node, to connect to it. Without
	// it, or for a node it does not know, the node is unreachable.
	Resolve func(node string) (addr string, ok bool)
	// Codec encodes the messages; Gob if nil.
	Codec Codec
	// TickTime is how long a connection may stay silent before it is
	// taken for lost, like net_ticktime: a connection idle sends a tick
	// four times as often. A send blocked as long fails it too. Zero
	// means DefaultTickTime.
	TickTime time.Duration
}

// DefaultTickTime is Config.TickTime when zero.
const DefaultTickTime = 60 * time.Second

// Dist is the distribution of a node, the counterpart of net_kernel: it
// connects the node to the others it sends to, and accepts their
// connections.
type Dist struct {
	n      *proc.Node
	cfg    Config
	remote proc.Remote
	ln     net.Listener

	mu      sync.Mutex
	peers   map[string]*peer
	stopped bool
}

// peer is the connection to a node, run by a process of its own. Its
// fields are guarded by Dist.mu.
type peer struct {
	node     string
	pid      proc.PID
	dialing  bool     // its own dial is in progress
	awaiting bool     // the node is to dial, having refused our dial
	incoming bool     // a connection accepted from the node is to be used
	ready    net.Conn // a connection made, for the process to take
	up       bool
}

var errUnnamed = errors.New("dist: the node needs a name of its own")

// Start distributes n. The node must have a name of its own, by which the
// others reach it.
func Start(n *proc.Node, cfg Config) (*Dist, error) {
	if n.Name() == proc.DefaultNodeName {
		return nil, errUnnamed
	}
	if cfg.Codec == nil {
		cfg.Codec = Gob
	}
	if cfg.TickTime <= 0 {
		cfg.TickTime = DefaultTickTime
	}
	d := &Dist{n: n, cfg: cfg, peers: make(map[string]*peer)}
	if cfg.Listen != "" {
		ln, err := net.Listen("tcp", cfg.Listen)
		if err != nil {
			return nil, err
		}
		d.ln = ln
	}
	d.remote = n.Distribute(distribution{d})
	if d.ln != nil {
		go d.acceptLoop()
	}
	return d, nil
}

// Addr returns the address the node accepts connections on, if any.
func (d *Dist) Addr() net.Addr {
	if d.ln == nil {
		return nil
	}
	return d.ln.Addr()
}

// Connect connects to node, if not connected yet, like
// net_kernel:connect_node. Sending to a node connects to it all the same;
// Connect waits for the connection, and tells whether it was made.
func (d *Dist) Connect(ctx context.Context, node string) error {
	if node == d.n.Name() {
		return nil
	}
	pid, ok := d.peerFor(node)
	if !ok {
		return proc.NoConnection
	}
	a := d.n.MonitorAlias(pid)
	defer a.Release()
	d.n.Send(pid, waitUp{a.Ref})
	select {
	case m := <-a.C:
		if m.Down {
			return proc.NoConnection
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Nodes returns the nodes connected, like erlang:nodes().
func (d *Dist) Nodes() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var nodes []string
	for name, p := range d.peers {
		if p.up {
			nodes = append(nodes, name)
		}
	}
	slices.Sort(nodes)
	return nodes
}

// Disconnect drops the connection to node, like
// erlang:disconnect_node: the links and monitors through it fail with
// proc.NoConnection. It returns once they have.
func (d *Dist) Disconnect(node string) {
	d.mu.Lock()
	p := d.peers[node]
	d.mu.Unlock()
	if p != nil {
		d.stopPeers([]*peer{p})
	}
}

// Stop stops the distribution: it accepts no more connections, and drops
// those it has. The node stays distributed, but reaches no other node.
func (d *Dist) Stop() {
	d.mu.Lock()
	d.stopped = true
	peers := make([]*peer, 0, len(d.peers))
	for _, p := range d.peers {
		peers = append(peers, p)
	}
	d.mu.Unlock()
	if d.ln != nil {
		d.ln.Close()
	}
	d.stopPeers(peers)
}

func (d *Dist) stopPeers(peers []*peer) {
	var waits []context.Context
	for _, p := range peers {
		ctx, cancel := d.n.Watch(context.Background(), p.pid)
		defer cancel()
		waits = append(waits, ctx)
		d.n.Send(p.pid, stop{})
	}
	for _, ctx := range waits {
		<-ctx.Done()
	}
}

// peerFor returns the process of the connection to node, starting one if
// there is none. It fails if the distribution is stopped.
func (d *Dist) peerFor(node string) (proc.PID, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return proc.PID{}, false
	}
	p := d.peers[node]
	if p == nil {
		p = d.startPeer(node, true)
	}
	return p.pid, true
}

// startPeer starts the process of the connection to node, dialing it or
// waiting for a connection accepted from it. d.mu is held.
func (d *Dist) startPeer(node string, dial bool) *peer {
	p := &peer{node: node, dialing: dial, incoming: !dial}
	d.peers[node] = p
	p.pid = d.n.Spawn(func(self *proc.Self) error { return d.runPeer(self, p) })
	return p
}

// out sends f to the connection to node.
func (d *Dist) out(node string, f frame) {
	pid, ok := d.peerFor(node)
	if !ok {
		// Not from here: the caller may hold the locks of a process.
		go d.remote.NodeDown(node)
		return
	}
	d.n.Send(pid, f)
}

func (d *Dist) acceptLoop() {
	for {
		conn, err := d.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			time.Sleep(10 * time.Millisecond) // out of descriptors, say
			continue
		}
		go d.accept(conn)
	}
}

// accept runs the handshake of a connection from another node, and hands
// it to the process of the connection to that node.
func (d *Dist) accept(conn net.Conn) {
	var chosen *peer
	_, err := acceptHandshake(conn, d.hello(), d.cfg.Cookie, func(info peerInfo) string {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.stopped {
			return "stopping"
		}
		if info.name == d.n.Name() || info.name == "" {
			return "same name"
		}
		p := d.peers[info.name]
		switch {
		case p == nil:
			p = d.startPeer(info.name, false)
		case p.up || p.incoming || p.ready != nil:
			return "already connected"
		case info.name < d.n.Name():
			// Both dial: the dial of the node with the smaller name wins,
			// here as there. A node waiting for it, its own dial refused,
			// has the larger name.
			p.incoming = true
		default:
			return refuseSimultaneous
		}
		chosen = p
		return ""
	})
	if chosen == nil {
		conn.Close()
		return
	}
	d.mu.Lock()
	chosen.incoming, chosen.awaiting = false, false
	if err == nil {
		d.handOver(chosen, conn)
	} else {
		conn.Close()
	}
	d.mu.Unlock()
	d.n.Send(chosen.pid, wake{})
}

// dial connects to the node of p and runs the handshake.
func (d *Dist) dial(p *peer) {
	conn, _, err := d.dialNode(p.node)
	d.mu.Lock()
	p.dialing = false
	switch {
	case err == nil && p.incoming:
		conn.Close() // the connection of the other node wins
	case err == nil:
		d.handOver(p, conn)
	case errors.Is(err, errSimultaneous):
		// The node dials too, and wins: wait for its connection, for a
		// while.
		p.awaiting = true
		time.AfterFunc(handshakeTimeout, func() {
			d.mu.Lock()
			p.awaiting = false
			d.mu.Unlock()
			d.n.Send(p.pid, wake{})
		})
	default:
		d.n.Logger().Debug("dist: cannot connect", "node", p.node, "error", err)
	}
	d.mu.Unlock()
	d.n.Send(p.pid, wake{})
}

// handOver leaves conn for the process of p to take, unless p is gone or
// has a connection already. d.mu is held.
func (d *Dist) handOver(p *peer, conn net.Conn) {
	if d.peers[p.node] != p || p.up || p.ready != nil {
		conn.Close()
		return
	}
	p.ready = conn
}

var errUnknownNode = errors.New("dist: unknown node")

func (d *Dist) dialNode(node string) (net.Conn, peerInfo, error) {
	if d.cfg.Resolve == nil {
		return nil, peerInfo{}, errUnknownNode
	}
	addr, ok := d.cfg.Resolve(node)
	if !ok {
		return nil, peerInfo{}, errUnknownNode
	}
	conn, err := net.DialTimeout("tcp", addr, handshakeTimeout)
	if err != nil {
		return nil, peerInfo{}, err
	}
	info, err := dialHandshake(conn, d.hello(), d.cfg.Cookie)
	if err == nil && info.name != node {
		err = errors.New("dist: " + addr + " is " + info.name + ", not " + node)
	}
	if err != nil {
		conn.Close()
		return nil, peerInfo{}, err
	}
	return conn, info, nil
}

func (d *Dist) hello() hello {
	return hello{name: d.n.Name(), creation: d.n.Creation(), challenge: newChallenge()}
}

// distribution is the proc.Distribution of a Dist.
type distribution struct{ d *Dist }

func (x distribution) Send(to proc.PID, msg any) {
	x.d.out(to.Node(), frame{op: opSend, to: to, msg: msg})
}

func (x distribution) SendName(node, name string, msg any) {
	x.d.out(node, frame{op: opSendName, name: name, msg: msg})
}

func (x distribution) SendAlias(ref proc.Ref, msg any) {
	x.d.out(ref.Node(), frame{op: opSendAlias, ref: ref, msg: msg})
}

func (x distribution) Exit(from, to proc.PID, reason error, viaLink bool) {
	o := opExit
	if viaLink {
		o = opExitLink
	}
	x.d.out(to.Node(), frame{op: o, from: from, to: to, reason: reason})
}

func (x distribution) Link(from, to proc.PID) {
	x.d.out(to.Node(), frame{op: opLink, from: from, to: to})
}

func (x distribution) Unlink(from, to proc.PID) {
	x.d.out(to.Node(), frame{op: opUnlink, from: from, to: to})
}

func (x distribution) Monitor(ref proc.Ref, target proc.PID) {
	x.d.out(target.Node(), frame{op: opMonitor, ref: ref, to: target})
}

func (x distribution) Demonitor(ref proc.Ref, target proc.PID) {
	x.d.out(target.Node(), frame{op: opDemonitor, ref: ref, to: target})
}

func (x distribution) MonitorName(ref proc.Ref, node, name string) {
	x.d.out(node, frame{op: opMonitorName, ref: ref, name: name})
}

func (x distribution) DemonitorName(ref proc.Ref, node, name string) {
	x.d.out(node, frame{op: opDemonitorName, ref: ref, name: name})
}

func (x distribution) Down(ref proc.Ref, target proc.PID, reason error) {
	x.d.out(ref.Node(), frame{op: opDown, ref: ref, to: target, reason: reason})
}
