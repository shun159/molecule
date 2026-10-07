package main

import (
	"hash/fnv"
	"maps"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

// Server is a Raft server: leader election and log replication, as in
// figure 2 of the paper, written as a gen_statem whose states are the
// roles. It is registered as "raft" on its node, and reaches the others
// by that name.
//
// Every callback is a pure function of the role, the data and the event:
// the timeouts are state timeouts, the random ones drawn from a generator
// kept in the data, and the messages to the other servers are effects.
type Server struct {
	Node  string   // the node of this server
	Peers []string // the nodes of the others
	Seed  uint64   // of the random election timeouts
	Disk  Persistent

	// A follower waits for a leader between ElectionTimeout and twice
	// it; a leader sends heartbeats every Heartbeat.
	ElectionTimeout time.Duration
	Heartbeat       time.Duration
}

// Role is the state of a server.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string { return [...]string{"follower", "candidate", "leader"}[r] }

// Entry is an entry of the log: a command, and the term of the leader that
// took it.
type Entry struct {
	Term uint64
	Cmd  string
}

// Persistent is what a server keeps on stable storage. Here, its data is
// that storage: what a server has once it has handled a message counts as
// written, and a server restarting is given it back as Disk.
type Persistent struct {
	Term     uint64
	VotedFor string
	Log      []Entry // entries are numbered from 1
}

// Data is the state of a server, besides its role. Its slices and maps
// are copied when changed, never written in place: an older state is
// never changed.
type Data struct {
	Persistent
	Commit int    // the last entry committed
	Leader string // the leader of the term, if known

	votes []string       // a candidate's
	next  map[string]int // a leader's: the next entry to send each peer
	match map[string]int // a leader's: the last entry each peer has
	rng   rand.PCG
}

// Messages between servers.
type (
	RequestVote struct {
		Term      uint64
		Candidate string
		LastIndex int
		LastTerm  uint64
	}
	Vote struct {
		Term    uint64
		From    string
		Granted bool
	}
	AppendEntries struct {
		Term      uint64
		Leader    string
		PrevIndex int
		PrevTerm  uint64
		Entries   []Entry
		Commit    int
	}
	Appended struct {
		Term    uint64
		From    string
		Success bool
		Match   int // the last entry the follower has like the leader
	}
)

// Requests of clients.
type (
	// Propose asks the leader to append Cmd to the log. Other servers drop
	// it: a client finds the leader with GetStatus.
	Propose struct{ Cmd string }
	// GetStatus asks a server its Status.
	GetStatus struct{}
	Status    struct {
		Role      Role
		Term      uint64
		Leader    string
		Committed []Entry
	}
)

// Timeouts.
type (
	electionTimeout struct{}
	heartbeat       struct{}
)

const raftName = "raft"

func (Server) StateEnter() bool { return true }

func (s Server) Init(proc.PID) (Role, Data, []gen.Effect, error) {
	h := fnv.New64a()
	h.Write([]byte(s.Node))
	return Follower, Data{Persistent: s.Disk, rng: *rand.NewPCG(s.Seed, h.Sum64())}, nil, nil
}

func (s Server) HandleEvent(role Role, d Data, ev genstatem.Event) (Role, Data, []gen.Effect) {
	switch e := ev.(type) {
	case genstatem.Enter[Role]:
		return s.enter(role, d)
	case genstatem.StateTimeout:
		switch e.Msg.(type) {
		case electionTimeout:
			return s.elect(d)
		case heartbeat:
			return role, d, s.heartbeats(d)
		}
	case genstatem.Call:
		if _, ok := e.Req.(GetStatus); ok {
			st := Status{Role: role, Term: d.Term, Leader: d.Leader, Committed: slices.Clone(d.Log[:d.Commit])}
			return role, d, gen.Do(e.Reply(st))
		}
	case genstatem.Cast:
		return s.receive(role, d, e.Msg)
	}
	return role, d, nil
}

func (s Server) enter(role Role, d Data) (Role, Data, []gen.Effect) {
	switch role {
	case Follower:
		t := s.electionTimer(&d)
		return role, d, gen.Do(t)
	case Leader:
		d.Leader = s.Node
		d.next, d.match = map[string]int{}, map[string]int{}
		for _, p := range s.Peers {
			d.next[p] = len(d.Log) + 1
		}
		return role, d, s.heartbeats(d)
	}
	return role, d, nil // a candidate's timer is set by elect
}

// elect starts an election, for the next term.
func (s Server) elect(d Data) (Role, Data, []gen.Effect) {
	d.Term++
	d.VotedFor, d.Leader, d.votes = s.Node, "", []string{s.Node}
	if s.majority(1) {
		return Leader, d, nil // alone
	}
	effs := []gen.Effect{s.electionTimer(&d)}
	req := RequestVote{Term: d.Term, Candidate: s.Node, LastIndex: len(d.Log), LastTerm: d.termAt(len(d.Log))}
	for _, p := range s.Peers {
		effs = append(effs, s.to(p, req))
	}
	return Candidate, d, effs
}

// receive handles a message from another server, or a proposal.
func (s Server) receive(role Role, d Data, msg any) (Role, Data, []gen.Effect) {
	if t := termOf(msg); t > d.Term {
		// A newer term: whatever this server was, it follows now.
		d.Term, d.VotedFor, d.Leader = t, "", ""
		role = Follower
	}
	switch m := msg.(type) {
	case RequestVote:
		return s.requestVote(role, d, m)
	case Vote:
		if role != Candidate || m.Term != d.Term || !m.Granted || slices.Contains(d.votes, m.From) {
			return role, d, nil
		}
		d.votes = append(slices.Clip(d.votes), m.From)
		if s.majority(len(d.votes)) {
			return Leader, d, nil
		}
	case AppendEntries:
		return s.appendEntries(role, d, m)
	case Appended:
		return s.appended(role, d, m)
	case Propose:
		if role != Leader {
			return role, d, nil
		}
		d.Log = append(slices.Clip(d.Log), Entry{Term: d.Term, Cmd: m.Cmd})
		d = s.advanceCommit(d)
		var effs []gen.Effect
		for _, p := range s.Peers {
			effs = append(effs, s.appendTo(d, p))
		}
		return role, d, effs
	}
	return role, d, nil
}

func (s Server) requestVote(role Role, d Data, m RequestVote) (Role, Data, []gen.Effect) {
	last := d.termAt(len(d.Log))
	upToDate := m.LastTerm > last || m.LastTerm == last && m.LastIndex >= len(d.Log)
	grant := m.Term == d.Term && (d.VotedFor == "" || d.VotedFor == m.Candidate) && upToDate
	var effs []gen.Effect
	if grant {
		d.VotedFor = m.Candidate
		if role == Follower {
			effs = append(effs, s.electionTimer(&d)) // a candidate may win: wait for it
		}
	}
	return role, d, append(effs, s.to(m.Candidate, Vote{Term: d.Term, From: s.Node, Granted: grant}))
}

func (s Server) appendEntries(role Role, d Data, m AppendEntries) (Role, Data, []gen.Effect) {
	refuse := func() []gen.Effect {
		return []gen.Effect{s.to(m.Leader, Appended{Term: d.Term, From: s.Node})}
	}
	if m.Term < d.Term {
		return role, d, refuse()
	}
	// The leader of this term: follow it, and wait for it again.
	var effs []gen.Effect
	if role == Follower {
		effs = append(effs, s.electionTimer(&d))
	}
	role, d.Leader = Follower, m.Leader
	if m.PrevIndex > len(d.Log) || d.termAt(m.PrevIndex) != m.PrevTerm {
		return role, d, append(effs, refuse()...)
	}
	log := slices.Clip(d.Log)
	for i, e := range m.Entries {
		at := m.PrevIndex + 1 + i
		if at <= len(log) {
			if log[at-1].Term == e.Term {
				continue
			}
			log = log[: at-1 : at-1] // a conflict: drop it and what follows
		}
		log = append(log, e)
	}
	d.Log = log
	last := m.PrevIndex + len(m.Entries)
	if c := min(m.Commit, last); c > d.Commit {
		d.Commit = c
	}
	return role, d, append(effs, s.to(m.Leader, Appended{Term: d.Term, From: s.Node, Success: true, Match: last}))
}

func (s Server) appended(role Role, d Data, m Appended) (Role, Data, []gen.Effect) {
	if role != Leader || m.Term != d.Term {
		return role, d, nil
	}
	if !m.Success {
		// The follower's log differs before what was sent: send from one
		// entry earlier.
		d.next = maps.Clone(d.next)
		d.next[m.From] = max(1, d.next[m.From]-1)
		return role, d, gen.Do(s.appendTo(d, m.From))
	}
	if m.Match > d.match[m.From] {
		d.next, d.match = maps.Clone(d.next), maps.Clone(d.match)
		d.match[m.From], d.next[m.From] = m.Match, m.Match+1
	}
	return role, s.advanceCommit(d), nil
}

// advanceCommit commits the last entry of the current term a majority
// has, and so all before it.
func (s Server) advanceCommit(d Data) Data {
	for n := len(d.Log); n > d.Commit && d.Log[n-1].Term == d.Term; n-- {
		have := 1
		for _, p := range s.Peers {
			if d.match[p] >= n {
				have++
			}
		}
		if s.majority(have) {
			d.Commit = n
			break
		}
	}
	return d
}

func (s Server) heartbeats(d Data) []gen.Effect {
	effs := []gen.Effect{genstatem.StartStateTimeout{After: s.Heartbeat, Msg: heartbeat{}}}
	for _, p := range s.Peers {
		effs = append(effs, s.appendTo(d, p))
	}
	return effs
}

// appendTo sends peer the entries it lacks, as far as the leader knows.
func (s Server) appendTo(d Data, peer string) gen.Effect {
	prev := d.next[peer] - 1
	return s.to(peer, AppendEntries{
		Term:      d.Term,
		Leader:    s.Node,
		PrevIndex: prev,
		PrevTerm:  d.termAt(prev),
		Entries:   d.Log[prev:len(d.Log):len(d.Log)],
		Commit:    d.Commit,
	})
}

// electionTimer starts the election timeout, of a random length.
func (s Server) electionTimer(d *Data) gen.Effect {
	after := s.ElectionTimeout + time.Duration(d.rng.Uint64()%uint64(s.ElectionTimeout))
	return genstatem.StartStateTimeout{After: after, Msg: electionTimeout{}}
}

func (s Server) majority(n int) bool { return 2*n > len(s.Peers)+1 }

func (s Server) to(node string, msg any) gen.Effect {
	return gen.Cast{To: gen.Remote{Node: node, Name: raftName}, Req: msg}
}

func (d Data) termAt(i int) uint64 {
	if i == 0 {
		return 0
	}
	return d.Log[i-1].Term
}

func termOf(msg any) uint64 {
	switch m := msg.(type) {
	case RequestVote:
		return m.Term
	case Vote:
		return m.Term
	case AppendEntries:
		return m.Term
	case Appended:
		return m.Term
	}
	return 0
}
