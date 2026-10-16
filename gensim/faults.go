package gensim

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/shun159/molecule/proc"
)

// Nodes fail as in dist: when two nodes lose each other, what was in
// flight between them is lost, and the links and monitors between their
// processes fail with proc.NoConnection, on both sides.

// Partition cuts the nodes of each group from those of the others; the
// nodes of a group still reach each other.
func (s *Sim) Partition(groups ...[]string) {
	for i, g := range groups {
		for _, h := range groups[i+1:] {
			for _, x := range g {
				for _, y := range h {
					if x == y || s.cuts[[2]string{x, y}] {
						continue
					}
					was := s.connected(x, y)
					s.cuts[[2]string{x, y}], s.cuts[[2]string{y, x}] = true, true
					s.record(Event{Kind: Fault, Msg: fmt.Sprintf("partition %s | %s", x, y)})
					if was {
						s.nodeEvent(x, y, false)
					}
					s.disconnect(x, y)
				}
			}
		}
	}
	s.listen()
}

// Heal undoes the partitions and the silences: all nodes up reach each
// other again, and what was held arrives.
func (s *Sim) Heal() {
	cut := s.cuts
	s.cuts = make(map[[2]string]bool)
	s.held = make(map[[2]string]bool)
	s.record(Event{Kind: Fault, Msg: "heal"})
	for _, x := range s.nodeNames() {
		for _, y := range s.nodeNames() {
			healed := cut[[2]string{x, y}] && s.connected(x, y)
			if x < y && healed { // each pair once
				s.nodeEvent(x, y, true)
			}
		}
	}
	s.listen()
}

// Crash stops the node name: its processes vanish, without running
// Terminate, and the other nodes see them gone with proc.NoConnection.
func (s *Sim) Crash(name string) {
	n := s.nodes[name]
	if n == nil || !n.up {
		return
	}
	s.record(Event{Kind: Fault, Msg: "crash " + name})
	for _, other := range s.nodeNames() {
		if other != name {
			if s.connected(name, other) {
				s.nodeEvent(name, other, false)
			}
			s.disconnect(name, other)
		}
	}
	n.up = false
	delete(s.paused, name)
	s.listen()
	for _, l := range s.links {
		if l.from.Node() == name || l.to.Node() == name {
			s.dropQueue(l)
		}
	}
	for _, p := range s.procs {
		if p.node == n && p.alive {
			p.alive = false
			p.runner.Abort()
			p.mailbox = nil
			s.record(Event{Kind: Exited, To: p.pid, Msg: ErrNodeDown})
		}
	}
	clear(n.names)
	// The requests of other nodes to it failed as disconnected; those of
	// the driver are not pending, Call being synchronous.
	for _, ref := range s.aliasRefs() {
		if s.aliases[ref].owner.Node() == name {
			s.releaseAlias(ref)
		}
	}
}

// Restart starts the node name again, crashed, as a new incarnation:
// the PIDs of its previous one never come back. It runs no process; the
// test spawns them again, with what they kept of their state, if any.
func (s *Sim) Restart(name string) {
	n := s.nodes[name]
	if n == nil || n.up {
		return
	}
	s.record(Event{Kind: Fault, Msg: "restart " + name})
	n.creation++
	n.alloc = proc.NewNode(name, proc.WithCreation(n.creation))
	n.up = true
	for _, other := range s.nodeNames() {
		if other != name && s.connected(name, other) {
			s.nodeEvent(name, other, true)
		}
	}
	s.listen()
}

// Skew sets the clock of the node name off by d from the simulation's,
// as a clock set wrong, or one that jumps: its Clocked processes read Now
// plus d. Timers are not skewed: they wait as long, as on a monotonic
// clock.
func (s *Sim) Skew(name string, d time.Duration) {
	s.record(Event{Kind: Fault, Msg: fmt.Sprintf("skew %s %v", name, d)})
	s.skews[name] = d
}

// connected reports whether the nodes x and y reach each other.
func (s *Sim) connected(x, y string) bool {
	nx, ny := s.nodes[x], s.nodes[y]
	return nx != nil && ny != nil && nx.up && ny.up && !s.cuts[[2]string{x, y}]
}

// nodeEvent tells the processes of x monitoring nodes that y is up or
// down, and those of y, that x is.
func (s *Sim) nodeEvent(x, y string, up bool) {
	for _, pid := range sortedPIDs(s.nodeWatchers) {
		p := s.byPID[pid]
		if p == nil || !p.alive {
			delete(s.nodeWatchers, pid)
			continue
		}
		var other string
		switch p.node.name {
		case x:
			other = y
		case y:
			other = x
		default:
			continue
		}
		if up {
			s.deliverLocal(p, proc.NodeUp{Node: other})
		} else {
			s.deliverLocal(p, proc.NodeDown{Node: other})
		}
	}
}

// disconnect makes the nodes x and y lose each other. What fails is
// gathered first, then told, so that a process dying of it does not
// signal across the cut as if connected.
func (s *Sim) disconnect(x, y string) {
	c := cut{x: x, y: y}
	for _, l := range s.links {
		if c.across(l.from, l.to) {
			s.dropQueue(l)
		}
	}
	for _, p := range s.procs {
		if p.alive && c.ends(p.node.name) {
			s.cutLinks(&c, p)
			s.cutMonitors(&c, p)
		}
	}
	c.aliases = s.aliasesAcross(c)
	s.tell(c)
}

// cut is what fails between the nodes x and y, cut off from each other.
type cut struct {
	x, y    string
	links   [][2]proc.PID // the processes linked across
	downs   []cutDown     // the monitors across
	aliases []proc.Ref    // the aliases of a monitor across
}

// cutDown is the DownMsg a watcher gets of a monitor across a cut.
type cutDown struct {
	watcher proc.PID
	down    proc.DownMsg
}

// ends reports whether node is x or y.
func (c cut) ends(node string) bool { return node == c.x || node == c.y }

// across reports whether a and b are on either side of the cut.
func (c cut) across(a, b proc.PID) bool {
	an, bn := a.Node(), b.Node()
	return an == c.x && bn == c.y || an == c.y && bn == c.x
}

// cutLinks unlinks p from the processes it is linked to across c.
func (s *Sim) cutLinks(c *cut, p *process) {
	for _, q := range sortedPIDs(p.linked) {
		if !c.across(p.pid, q) {
			continue
		}
		delete(p.linked, q)
		if qp := s.byPID[q]; qp != nil {
			delete(qp.linked, p.pid)
		}
		c.links = append(c.links, [2]proc.PID{p.pid, q})
	}
}

// cutMonitors ends the monitors of p by processes across c.
func (s *Sim) cutMonitors(c *cut, p *process) {
	for _, ref := range sortedRefs(p.watchers) {
		w := p.watchers[ref]
		if w.alias || !c.across(w.pid, p.pid) {
			continue
		}
		delete(p.watchers, ref)
		if wp := s.byPID[w.pid]; wp != nil {
			delete(wp.watching, ref)
		}
		c.downs = append(c.downs, cutDown{w.pid, proc.DownMsg{Ref: ref, PID: p.pid, Reason: proc.NoConnection}})
	}
}

// aliasesAcross returns the aliases of a monitor across c.
func (s *Sim) aliasesAcross(c cut) []proc.Ref {
	var refs []proc.Ref
	for _, ref := range s.aliasRefs() {
		if a := s.aliases[ref]; !a.owner.IsZero() && c.across(a.owner, a.target) {
			refs = append(refs, ref)
		}
	}
	return refs
}

// tell tells the processes alive what c failed: each end of a link its
// exit signal, each watcher its Down, and each alias its down.
func (s *Sim) tell(c cut) {
	for _, l := range c.links {
		for _, end := range [][2]proc.PID{{l[0], l[1]}, {l[1], l[0]}} {
			if p := s.alive(end[0]); p != nil {
				s.signalLocal(p, end[1], proc.NoConnection)
			}
		}
	}
	for _, d := range c.downs {
		if p := s.alive(d.watcher); p != nil {
			s.deliverLocal(p, d.down)
		}
	}
	for _, ref := range c.aliases {
		if a := s.aliases[ref]; a != nil {
			s.answer(ref, a.owner, proc.AliasMsg{Down: true, Reason: proc.NoConnection})
		}
	}
}

// dropQueue loses what is in flight on l.
func (s *Sim) dropQueue(l *link) {
	for _, f := range l.queue {
		s.record(Event{Kind: Dropped, From: l.from, To: l.to, Msg: f.msg})
	}
	l.queue = nil
}

// deliverLocal puts msg in the mailbox of p, from its own node.
func (s *Sim) deliverLocal(p *process, msg any) {
	s.record(Event{Kind: Sent, To: p.pid, Msg: msg})
	s.enqueue(p, msg)
}

// signalLocal delivers to p an exit signal its node raised, such as
// proc.NoConnection for a link lost.
func (s *Sim) signalLocal(p *process, from proc.PID, reason error) {
	if p.trap {
		s.deliverLocal(p, proc.ExitMsg{From: from, Reason: reason})
		return
	}
	s.exit(p, reason)
}

func (s *Sim) nodeNames() []string {
	names := make([]string, 0, len(s.nodes))
	for name := range s.nodes {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (s *Sim) aliasRefs() []proc.Ref {
	refs := make([]proc.Ref, 0, len(s.aliases))
	for ref := range s.aliases {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b proc.Ref) int { return cmp.Compare(a.String(), b.String()) })
	return refs
}

func sortedRefs[V any](m map[proc.Ref]V) []proc.Ref {
	refs := make([]proc.Ref, 0, len(m))
	for ref := range m {
		refs = append(refs, ref)
	}
	slices.SortFunc(refs, func(a, b proc.Ref) int { return cmp.Compare(a.String(), b.String()) })
	return refs
}
