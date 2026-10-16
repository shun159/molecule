package gensim

import (
	"fmt"
	"time"

	"github.com/shun159/molecule/proc"
)

// Nodes also fall silent, as they do with dist, the connection staying:
// a node stopped a while, or a network that holds what is sent, as TCP
// does, rather than losing the connection. The other nodes hear nothing
// from it for Silent, and a quarter more at most, drawn for each, then
// are told it silent, a proc.NodeSilent; heard again, a proc.NodeHeard.
// What was sent is held, not lost: it arrives once heard, in order.

// DefaultSilent is Sim.Silent unless set, as dist's.
const DefaultSilent = time.Second

// Pause stops the node name a while, as a stop signal or a long pause of
// its runtime does: its processes handle nothing, what is sent to them
// held, their timers firing into their mailboxes; the other nodes are
// told it silent, after Silent. Resume starts it again.
func (s *Sim) Pause(name string) {
	if n := s.nodes[name]; n == nil || !n.up {
		return // gone, or down
	}
	if s.paused[name] {
		return
	}
	s.record(Event{Kind: Fault, Msg: "pause " + name})
	s.paused[name] = true
	s.listen()
}

// Resume starts the node name again, paused: its processes handle the
// timers that fired meanwhile, then what was held for them, and the other
// nodes hear from it again.
func (s *Sim) Resume(name string) {
	if !s.paused[name] {
		return
	}
	s.record(Event{Kind: Fault, Msg: "resume " + name})
	delete(s.paused, name)
	s.listen()
}

// Silence holds what the nodes of each group send those of the others,
// as a network that stops carrying it, the connections staying: the
// nodes are told each other silent, after Silent. Heal delivers what was
// held. Partition, rather, loses it, the connections lost at once.
func (s *Sim) Silence(groups ...[]string) {
	for i, g := range groups {
		for _, h := range groups[i+1:] {
			for _, x := range g {
				for _, y := range h {
					if x == y || s.held[[2]string{x, y}] {
						continue
					}
					s.held[[2]string{x, y}], s.held[[2]string{y, x}] = true, true
					s.record(Event{Kind: Fault, Msg: fmt.Sprintf("silence %s | %s", x, y)})
				}
			}
		}
	}
	s.listen()
}

// flows reports whether what is in flight on l can arrive: nothing holds
// it between its nodes, and its receiver's node is not paused.
func (s *Sim) flows(l *link) bool {
	to := l.to.Node()
	if s.paused[to] {
		return false
	}
	return l.from.IsZero() || !s.held[[2]string{l.from.Node(), to}]
}

// hears reports whether the node x hears from y, connected: nothing held
// between them, and y not paused.
func (s *Sim) hears(x, y string) bool {
	return !s.held[[2]string{x, y}] && !s.paused[y]
}

// listen has each node, connected to another, it no longer hears from,
// told it silent after Silent, unless it hears from it again before; and
// told heard the node it was told silent, once it hears from it again.
func (s *Sim) listen() {
	names := s.nodeNames()
	for _, x := range names {
		for _, y := range names {
			if x == y {
				continue
			}
			pair := [2]string{x, y}
			switch {
			case !s.connected(x, y): // told down, if anything
				s.unsilence(pair)
				delete(s.told, pair)
			case s.hears(x, y):
				s.unsilence(pair)
				if s.told[pair] {
					delete(s.told, pair)
					s.tellNode(x, proc.NodeHeard{Node: y})
				}
			case !s.told[pair] && s.silences[pair] == nil:
				s.silences[pair] = s.after(s.Silent+time.Duration(s.rng.Int64N(int64(s.Silent/4)+1)), func() {
					delete(s.silences, pair)
					s.told[pair] = true
					s.tellNode(x, proc.NodeSilent{Node: y})
				})
			}
		}
	}
}

// unsilence cancels the telling of x that y is silent, pair being x, y.
func (s *Sim) unsilence(pair [2]string) {
	if t := s.silences[pair]; t != nil {
		t.cancelled = true
		delete(s.silences, pair)
	}
}

// tellNode tells msg to the processes of the node name monitoring nodes.
func (s *Sim) tellNode(name string, msg any) {
	for _, pid := range sortedPIDs(s.nodeWatchers) {
		p := s.byPID[pid]
		if p == nil || !p.alive {
			delete(s.nodeWatchers, pid)
			continue
		}
		if p.node.name == name {
			s.deliverLocal(p, msg)
		}
	}
}
