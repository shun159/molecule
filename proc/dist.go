package proc

import (
	"encoding/binary"
	"errors"
	"sync"
)

// Distribution carries to other nodes what the processes of a node send
// them, as the distribution of Erlang does. A node has none until a
// package such as dist gives it one with Node.Distribute; without it, the
// processes of other nodes are unreachable, and links and monitors to
// them fail at once with NoConnection.
//
// Its methods are called from any goroutine, must not block, and must
// keep what one process sends to another in order, messages and signals
// together. What cannot reach its node is dropped; the links and monitors
// it concerned then fail through Remote.NodeDown, which the distribution
// calls when it gives up on a node.
type Distribution interface {
	// Send sends msg to the process to, of another node.
	Send(to PID, msg any)
	// SendName sends msg to the process registered as name on node.
	SendName(node, name string, msg any)
	// SendAlias sends msg to the alias ref, of another node.
	SendAlias(ref Ref, msg any)
	// Exit sends an exit signal from from to to, through a link if
	// viaLink.
	Exit(from, to PID, reason error, viaLink bool)
	// Link and Unlink tell to that from has linked to it, or unlinked.
	Link(from, to PID)
	Unlink(from, to PID)
	// Monitor and Demonitor start and stop the monitor ref of target. The
	// node of ref is the node watching.
	Monitor(ref Ref, target PID)
	Demonitor(ref Ref, target PID)
	// MonitorName and DemonitorName do so for the process registered as
	// name on node, at the time.
	MonitorName(ref Ref, node, name string)
	DemonitorName(ref Ref, node, name string)
	// Down tells the node of ref that target, which it monitored with
	// ref, has died.
	Down(ref Ref, target PID, reason error)
}

// Distribute makes d the distribution of n, and returns the handle through
// which d hands n what other nodes send it. It is called once, before the
// node reaches other nodes.
func (n *Node) Distribute(d Distribution) Remote {
	if !n.dist.CompareAndSwap(nil, &d) {
		panic("proc: node already distributed")
	}
	return Remote{n}
}

func (n *Node) distribution() Distribution {
	if d := n.dist.Load(); d != nil {
		return *d
	}
	return nil
}

// remote returns the distribution to reach pid, if pid is of another node
// and n is distributed.
func (n *Node) remote(pid PID) Distribution {
	if pid.node == n.name || pid.IsZero() {
		return nil
	}
	return n.distribution()
}

// Creation returns the creation of n, which tells its incarnations apart.
func (n *Node) Creation() uint32 { return n.creation }

// SendName sends msg to the process registered as name on node, like
// erlang:send({Name, Node}, Msg). It never fails: the message is dropped
// if no process has the name, or node is unreachable.
func (n *Node) SendName(node, name string, msg any) {
	if node == n.name {
		if pid, ok := n.WhereIs(name); ok {
			n.Send(pid, msg)
		}
		return
	}
	if d := n.distribution(); d != nil {
		d.SendName(node, name, msg)
	}
}

// Remote is how a distribution hands a node what other nodes send it. It
// is plain: its methods act on the processes of the node as the senders'
// would, had they been local.
type Remote struct{ n *Node }

// Send delivers msg to the local process to.
func (r Remote) Send(to PID, msg any) {
	if p := r.n.lookup(to); p != nil {
		p.mbox.push(msg)
	}
}

// SendName delivers msg to the local process registered as name.
func (r Remote) SendName(name string, msg any) {
	if pid, ok := r.n.WhereIs(name); ok {
		r.Send(pid, msg)
	}
}

// SendAlias delivers msg to the local alias ref.
func (r Remote) SendAlias(ref Ref, msg any) {
	if ref.node == r.n.name {
		r.n.SendAlias(ref, msg)
	}
}

// Exit delivers to the local process to an exit signal from from.
func (r Remote) Exit(from, to PID, reason error, viaLink bool) {
	if p := r.n.lookup(to); p != nil {
		p.signalExit(from, reason, viaLink)
	}
}

// Link links from to the local process to. If to does not exist, from
// gets an exit signal with NoProc.
func (r Remote) Link(from, to PID) {
	if p := r.n.lookup(to); p != nil {
		p.mu.Lock()
		dead := p.dead.Load()
		if !dead {
			p.link(from)
		}
		p.mu.Unlock()
		if !dead {
			return
		}
	}
	if d := r.n.distribution(); d != nil {
		d.Exit(to, from, NoProc, true)
	}
}

// Unlink removes the link between from and the local process to.
func (r Remote) Unlink(from, to PID) {
	if p := r.n.lookup(to); p != nil {
		p.mu.Lock()
		delete(p.links, from)
		p.mu.Unlock()
	}
}

// Monitor starts the monitor ref of the local process target, for the node
// of ref. If target does not exist, that node is told it is down with
// NoProc.
func (r Remote) Monitor(ref Ref, target PID) {
	if p := r.n.lookup(target); p != nil {
		p.mu.Lock()
		dead := p.dead.Load()
		if !dead {
			p.watchedBy(ref, watcher{remote: true})
		}
		p.mu.Unlock()
		if !dead {
			return
		}
	}
	if d := r.n.distribution(); d != nil {
		d.Down(ref, target, NoProc)
	}
}

// MonitorName starts the monitor ref of the local process registered as
// name. If there is none, the node of ref is told it is down with NoProc.
func (r Remote) MonitorName(ref Ref, name string) {
	if pid, ok := r.n.WhereIs(name); ok {
		r.Monitor(ref, pid)
		return
	}
	if d := r.n.distribution(); d != nil {
		d.Down(ref, PID{node: r.n.name}, NoProc)
	}
}

// DemonitorName stops the monitor ref of the local process registered as
// name.
func (r Remote) DemonitorName(ref Ref, name string) {
	if pid, ok := r.n.WhereIs(name); ok {
		r.Demonitor(ref, pid)
	}
}

// Demonitor stops the monitor ref of the local process target.
func (r Remote) Demonitor(ref Ref, target PID) {
	if p := r.n.lookup(target); p != nil {
		p.mu.Lock()
		delete(p.monitors, ref)
		p.mu.Unlock()
	}
}

// Down tells the local watcher of ref that target has died.
func (r Remote) Down(ref Ref, target PID, reason error) {
	if m, ok := r.n.remoteMons.take(ref); ok {
		m.w.notify(r.n, ref, target, reason)
	}
}

// NodeDown tells that node is unreachable, its connection lost or never
// made: the local processes linked to its processes get an exit signal
// with NoConnection, the monitors of its processes fire with
// NoConnection, and its monitors of local processes are dropped.
func (r Remote) NodeDown(node string) {
	n := r.n
	for _, m := range n.remoteMons.takeNode(node) {
		m.w.notify(n, m.ref, m.target, NoConnection)
	}
	for _, p := range n.procs.all() {
		var from []PID
		p.mu.Lock()
		for pid := range p.links {
			if pid.node == node {
				from = append(from, pid)
			}
		}
		for ref, w := range p.monitors {
			if w.remote && ref.node == node {
				delete(p.monitors, ref)
			}
		}
		p.mu.Unlock()
		for _, pid := range from {
			p.signalExit(pid, NoConnection, true)
		}
	}
}

// remoteMon is a monitor of a process of another node, held by a local
// watcher.
type remoteMon struct {
	ref    Ref
	target PID
	w      watcher
}

// remoteMonTable holds the monitors of remote processes, by reference, for
// the Down that ends them to find their watcher.
type remoteMonTable struct {
	mu sync.Mutex
	m  map[Ref]remoteMon
}

func (t *remoteMonTable) put(ref Ref, target PID, w watcher) {
	t.mu.Lock()
	if t.m == nil {
		t.m = make(map[Ref]remoteMon)
	}
	t.m[ref] = remoteMon{ref, target, w}
	t.mu.Unlock()
}

func (t *remoteMonTable) take(ref Ref) (remoteMon, bool) {
	t.mu.Lock()
	m, ok := t.m[ref]
	delete(t.m, ref)
	t.mu.Unlock()
	return m, ok
}

func (t *remoteMonTable) takeNode(node string) []remoteMon {
	t.mu.Lock()
	defer t.mu.Unlock()
	var ms []remoteMon
	for ref, m := range t.m {
		if m.target.node == node {
			ms = append(ms, m)
			delete(t.m, ref)
		}
	}
	return ms
}

// monitorRemote starts a monitor of target, of another node, for w. It
// reports false if n is not distributed.
func (n *Node) monitorRemote(ref Ref, target PID, w watcher) bool {
	d := n.remote(target)
	if d == nil {
		return false
	}
	n.remoteMons.put(ref, target, w)
	d.Monitor(ref, target)
	return true
}

// MonitorAliasName is MonitorAlias for the process registered as name on
// node, which may be another: the process is the one with the name when
// the monitor reaches node. If there is none, C has a death with NoProc.
// A message sent to name after MonitorAliasName finds the monitor in
// place, so that a reply or the death of the process arrives.
func (n *Node) MonitorAliasName(node, name string) Alias {
	if node == n.name {
		if pid, ok := n.WhereIs(name); ok {
			return n.MonitorAlias(pid)
		}
		return n.downAlias(NoProc)
	}
	d := n.distribution()
	if d == nil {
		return n.downAlias(NoConnection)
	}
	ref := n.MakeRef()
	ch := make(chan AliasMsg, 1)
	n.aliases.put(ref.id, ch)
	target := PID{node: node}
	n.remoteMons.put(ref, target, watcher{alias: true})
	d.MonitorName(ref, node, name)
	return Alias{Ref: ref, C: ch, n: n, remote: target, remoteName: name}
}

// downAlias returns an alias whose process is already dead with reason.
func (n *Node) downAlias(reason error) Alias {
	ch := make(chan AliasMsg, 1)
	ch <- AliasMsg{Down: true, Reason: reason}
	return Alias{Ref: n.MakeRef(), C: ch, n: n}
}

// demonitorRemote stops the monitor ref of target, of another node.
func (n *Node) demonitorRemote(ref Ref, target PID) {
	if _, ok := n.remoteMons.take(ref); ok {
		if d := n.remote(target); d != nil {
			d.Demonitor(ref, target)
		}
	}
}

// PIDs and Refs travel between nodes as their node name, creation and
// serial number.

// MarshalBinary encodes p, for distribution.
func (p PID) MarshalBinary() ([]byte, error) { return marshalID(p.node, p.creation, p.id), nil }

// UnmarshalBinary decodes a PID encoded by MarshalBinary.
func (p *PID) UnmarshalBinary(b []byte) error {
	var err error
	p.node, p.creation, p.id, err = unmarshalID(b)
	return err
}

// MarshalBinary encodes r, for distribution.
func (r Ref) MarshalBinary() ([]byte, error) { return marshalID(r.node, r.creation, r.id), nil }

// UnmarshalBinary decodes a Ref encoded by MarshalBinary.
func (r *Ref) UnmarshalBinary(b []byte) error {
	var err error
	r.node, r.creation, r.id, err = unmarshalID(b)
	return err
}

func marshalID(node string, creation uint32, id uint64) []byte {
	b := make([]byte, 0, 2+len(node)+4+8)
	b = binary.BigEndian.AppendUint16(b, uint16(len(node)))
	b = append(b, node...)
	b = binary.BigEndian.AppendUint32(b, creation)
	return binary.BigEndian.AppendUint64(b, id)
}

var errBadID = errors.New("proc: bad encoded PID or Ref")

func unmarshalID(b []byte) (node string, creation uint32, id uint64, err error) {
	if len(b) < 2 {
		return "", 0, 0, errBadID
	}
	l := int(binary.BigEndian.Uint16(b))
	if len(b) != 2+l+12 {
		return "", 0, 0, errBadID
	}
	node = string(b[2 : 2+l])
	creation = binary.BigEndian.Uint32(b[2+l:])
	id = binary.BigEndian.Uint64(b[2+l+4:])
	return node, creation, id, nil
}
