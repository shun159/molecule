package gensim

import (
	"cmp"
	"fmt"
	"slices"

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
					s.cuts[[2]string{x, y}], s.cuts[[2]string{y, x}] = true, true
					s.record(Event{Kind: Fault, Msg: fmt.Sprintf("partition %s | %s", x, y)})
					s.disconnect(x, y)
				}
			}
		}
	}
}

// Heal undoes the partitions: all nodes up reach each other again.
func (s *Sim) Heal() {
	clear(s.cuts)
	s.record(Event{Kind: Fault, Msg: "heal"})
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
			s.disconnect(name, other)
		}
	}
	n.up = false
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
}

// disconnect makes the nodes x and y lose each other.
func (s *Sim) disconnect(x, y string) {
	between := func(a, b proc.PID) bool {
		an, bn := a.Node(), b.Node()
		return an == x && bn == y || an == y && bn == x
	}
	for _, l := range s.links {
		if between(l.from, l.to) {
			s.dropQueue(l)
		}
	}
	// What fails is gathered first, then told, so that a process dying of
	// it does not signal across the cut as if connected.
	type pair struct{ a, b proc.PID }
	var links []pair
	var downs []struct {
		watcher proc.PID
		down    proc.DownMsg
	}
	for _, p := range s.procs {
		if !p.alive || (p.node.name != x && p.node.name != y) {
			continue
		}
		for _, q := range sortedPIDs(p.linked) {
			if !between(p.pid, q) {
				continue
			}
			delete(p.linked, q)
			if qp := s.byPID[q]; qp != nil {
				delete(qp.linked, p.pid)
			}
			links = append(links, pair{p.pid, q})
		}
		for _, ref := range sortedRefs(p.watchers) {
			w := p.watchers[ref]
			if w.alias || !between(w.pid, p.pid) {
				continue
			}
			delete(p.watchers, ref)
			if wp := s.byPID[w.pid]; wp != nil {
				delete(wp.watching, ref)
			}
			downs = append(downs, struct {
				watcher proc.PID
				down    proc.DownMsg
			}{w.pid, proc.DownMsg{Ref: ref, PID: p.pid, Reason: proc.NoConnection}})
		}
	}
	var aliases []proc.Ref
	for _, ref := range s.aliasRefs() {
		if a := s.aliases[ref]; !a.owner.IsZero() && between(a.owner, a.target) {
			aliases = append(aliases, ref)
		}
	}
	for _, l := range links {
		for _, end := range [][2]proc.PID{{l.a, l.b}, {l.b, l.a}} {
			if p := s.byPID[end[0]]; p != nil && p.alive {
				s.signalLocal(p, end[1], proc.NoConnection)
			}
		}
	}
	for _, d := range downs {
		if p := s.byPID[d.watcher]; p != nil && p.alive {
			s.deliverLocal(p, d.down)
		}
	}
	for _, ref := range aliases {
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
	p.mailbox = append(p.mailbox, msg)
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
