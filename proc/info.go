package proc

import (
	"cmp"
	"slices"
)

// The processes of a node can be listed and looked into, like
// erlang:processes and erlang:process_info, to see what a running program
// does: what each process is, how much waits in its mailbox, what it is
// linked to.

// SetLabel tells what the process is, for those looking into the node,
// like the $initial_call of proc_lib: the runtime of a behaviour labels
// its process with the behaviour, say.
func (s *Self) SetLabel(label string) { s.p.label.Store(&label) }

// ProcessInfo is what Info tells of a process.
type ProcessInfo struct {
	PID   PID
	Name  string // registered, if any
	Label string // see Self.SetLabel
	// Parent is the process that started it, if any.
	Parent PID
	// MessageQueueLen is how many messages wait in its mailbox.
	MessageQueueLen int
	Links           []PID
	MonitoredBy     int // monitors of it
	Monitoring      int // its monitors of others
	TrapExit        bool
}

// Info returns what pid is and does, if a live process of n.
func (n *Node) Info(pid PID) (ProcessInfo, bool) {
	p := n.lookup(pid)
	if p == nil || p.dead.Load() {
		return ProcessInfo{}, false
	}
	return p.info(), true
}

func (p *process) info() ProcessInfo {
	info := ProcessInfo{
		PID:             p.pid,
		Parent:          p.parent,
		MessageQueueLen: p.mbox.len(),
		TrapExit:        p.trapExit.Load(),
	}
	if l := p.label.Load(); l != nil {
		info.Label = *l
	}
	p.node.regMu.Lock()
	info.Name = p.name
	p.node.regMu.Unlock()
	p.mu.Lock()
	for pid := range p.links {
		info.Links = append(info.Links, pid)
	}
	info.MonitoredBy, info.Monitoring = len(p.monitors), len(p.monitoring)
	p.mu.Unlock()
	slices.SortFunc(info.Links, comparePIDs)
	return info
}

// Processes returns the live processes of n, in the order they started.
func (n *Node) Processes() []PID {
	var pids []PID
	for _, p := range n.procs.all() {
		if !p.dead.Load() {
			pids = append(pids, p.pid)
		}
	}
	slices.SortFunc(pids, comparePIDs)
	return pids
}

func comparePIDs(a, b PID) int {
	return cmp.Or(cmp.Compare(a.node, b.node), cmp.Compare(a.creation, b.creation), cmp.Compare(a.id, b.id))
}

// NodeStats are counts of the processes of a node.
type NodeStats struct {
	Processes int    // alive
	Spawned   uint64 // ever
	Exited    uint64 // ever
	// Crashed counts the processes whose function failed, as reported,
	// rather than those killed or ended by an exit signal.
	Crashed uint64
}

// Stats returns counts of the processes of n.
func (n *Node) Stats() NodeStats {
	return NodeStats{
		Processes: len(n.Processes()),
		Spawned:   n.spawned.Load(),
		Exited:    n.exited.Load(),
		Crashed:   n.crashed.Load(),
	}
}
