package proc

import (
	"maps"
	"slices"
	"sync"
)

// NodeUp and NodeDown tell a process monitoring nodes that the connection
// to Node was made, or lost, like {nodeup, Node} and {nodedown, Node} in
// Erlang. NodeSilent tells that Node, connected, has not been heard from
// for a while, which the distribution says, and NodeHeard that it is
// again: the connection, and what goes through it, stays.
type (
	NodeUp     struct{ Node string }
	NodeDown   struct{ Node string }
	NodeSilent struct{ Node string }
	NodeHeard  struct{ Node string }
)

// MonitorNodes makes the process monitor the connections of its node to
// others, or stop, like net_kernel:monitor_nodes: a NodeUp arrives at once
// for each node connected, and a NodeSilent for each of them silent, then
// a NodeUp and a NodeDown each time a connection is made and lost, and a
// NodeSilent and a NodeHeard each time a node connected falls silent and
// is heard again. A node is told up once for each connection, and down
// once after it; silent and heard, in turn, in between.
func (s *Self) MonitorNodes(on bool) {
	p := s.p
	if p.dead.Load() {
		return
	}
	p.watchesNodes.Store(on)
	if on {
		p.node.nodes.watch(p)
	} else {
		p.node.nodes.unwatch(p.pid)
	}
}

// nodeTable keeps the nodes connected, and the processes monitoring them.
type nodeTable struct {
	mu       sync.Mutex
	conn     map[string]bool // the nodes connected, true if silent
	watchers map[PID]*process
}

func (t *nodeTable) watch(p *process) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.watchers == nil {
		t.watchers = make(map[PID]*process)
	}
	if _, ok := t.watchers[p.pid]; ok {
		return
	}
	t.watchers[p.pid] = p
	// Under the lock, so that what changes after comes after.
	for _, node := range slices.Sorted(maps.Keys(t.conn)) {
		p.push(NodeUp{node})
		if t.conn[node] {
			p.push(NodeSilent{node})
		}
	}
}

func (t *nodeTable) unwatch(pid PID) {
	t.mu.Lock()
	delete(t.watchers, pid)
	t.mu.Unlock()
}

func (t *nodeTable) up(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.conn[node]; ok {
		return
	}
	if t.conn == nil {
		t.conn = make(map[string]bool)
	}
	t.conn[node] = false
	t.tell(NodeUp{node})
}

func (t *nodeTable) down(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.conn[node]; !ok {
		return
	}
	delete(t.conn, node)
	t.tell(NodeDown{node})
}

// silenced tells that node, if connected, is silent, or heard again.
func (t *nodeTable) silenced(node string, silent bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	was, ok := t.conn[node]
	if !ok || was == silent {
		return
	}
	t.conn[node] = silent
	if silent {
		t.tell(NodeSilent{node})
	} else {
		t.tell(NodeHeard{node})
	}
}

// tell pushes msg to the watchers. t.mu is held.
func (t *nodeTable) tell(msg any) {
	for _, p := range t.watchers {
		p.push(msg)
	}
}
