package proc

import (
	"maps"
	"slices"
	"sync"
)

// NodeUp and NodeDown tell a process monitoring nodes that the connection
// to Node was made, or lost, like {nodeup, Node} and {nodedown, Node} in
// Erlang.
type (
	NodeUp   struct{ Node string }
	NodeDown struct{ Node string }
)

// MonitorNodes makes the process monitor the connections of its node to
// others, or stop, like net_kernel:monitor_nodes: a NodeUp arrives at once
// for each node connected, then a NodeUp and a NodeDown each time a
// connection is made and lost. A node is told up once for each connection,
// and down once after it.
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
	conn     map[string]bool // the nodes connected
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
		p.mbox.push(NodeUp{node})
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
	if t.conn[node] {
		return
	}
	if t.conn == nil {
		t.conn = make(map[string]bool)
	}
	t.conn[node] = true
	for _, p := range t.watchers {
		p.mbox.push(NodeUp{node})
	}
}

func (t *nodeTable) down(node string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.conn[node] {
		return
	}
	delete(t.conn, node)
	for _, p := range t.watchers {
		p.mbox.push(NodeDown{node})
	}
}
