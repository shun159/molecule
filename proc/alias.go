package proc

import "sync"

// AliasMsg is what arrives on the channel of an Alias: the message sent to
// it with SendAlias or, for an alias that monitors a process, the death of
// that process.
type AliasMsg struct {
	Msg    any
	Down   bool  // the monitored process died
	Reason error // its exit reason, when Down
}

// Alias is a one-shot address, like erlang:alias([reply]): the first
// message sent to Ref with SendAlias arrives on C and deactivates the
// alias. Unlike a PID it is not tied to a process, so code outside
// processes can receive through it; like a PID it is plain data, so it can
// be handed to a remote node.
//
// An Alias from MonitorAlias also watches a process, and its death
// arrives on C instead if it comes first. Exactly one AliasMsg ever
// arrives.
//
// Release must be called once the alias is no longer needed.
type Alias struct {
	Ref Ref
	C   <-chan AliasMsg

	n      *Node
	target *process // watched by a MonitorAlias, if any
	remote PID      // or watched there, of another node
}

// Alias creates an alias.
func (n *Node) Alias() Alias {
	ref := n.MakeRef()
	ch := make(chan AliasMsg, 1)
	n.aliases.put(ref.id, ch)
	return Alias{Ref: ref, C: ch, n: n}
}

// MonitorAlias creates an alias that also monitors pid, like
// monitor(process, Pid, [{alias, demonitor}]) in Erlang: on C arrives
// either the reply sent to Ref, or the death of pid, whichever is first.
// A reply sent by pid before it died always comes first. If pid does not
// exist, C has its death with NoProc at once.
//
// It is what a request waiting for a reply needs, and lighter than Watch
// plus Alias.
func (n *Node) MonitorAlias(pid PID) Alias {
	ref := n.MakeRef()
	ch := make(chan AliasMsg, 1)
	a := Alias{Ref: ref, C: ch, n: n}

	t := n.lookup(pid)
	if t == nil && n.remote(pid) != nil {
		n.aliases.put(ref.id, ch)
		n.monitorRemote(ref, pid, watcher{alias: true})
		a.remote = pid
		return a
	}
	if t == nil {
		reason := NoProc
		if !n.isLocal(pid) {
			reason = NoConnection
		}
		ch <- AliasMsg{Down: true, Reason: reason}
		return a
	}

	n.aliases.put(ref.id, ch)
	t.mu.Lock()
	if t.dead.Load() {
		t.mu.Unlock()
		n.aliasDown(ref, NoProc)
		return a
	}
	t.watchedBy(ref, watcher{alias: true})
	t.mu.Unlock()
	a.target = t
	return a
}

// Release deactivates the alias and its monitor, if any. A message
// already on C stays there.
func (a Alias) Release() {
	a.n.aliases.del(a.Ref.id)
	if t := a.target; t != nil {
		t.mu.Lock()
		delete(t.monitors, a.Ref)
		t.mu.Unlock()
	}
	if a.remote.node != "" {
		a.n.demonitorRemote(a.Ref, a.remote)
	}
}

// SendAlias delivers msg to the alias ref. Like Send it never fails:
// messages to inactive, stale or unreachable aliases are dropped.
func (n *Node) SendAlias(ref Ref, msg any) {
	if ref.node != n.name {
		if d := n.distribution(); d != nil && !ref.IsZero() {
			d.SendAlias(ref, msg)
		}
		return
	}
	if ref.creation != n.creation {
		return
	}
	if ch, ok := n.aliases.take(ref.id); ok {
		ch <- AliasMsg{Msg: msg} // never blocks: only the first gets here
	}
}

// aliasDown delivers the death of the process watched by alias ref.
func (n *Node) aliasDown(ref Ref, reason error) {
	if ch, ok := n.aliases.take(ref.id); ok {
		ch <- AliasMsg{Down: true, Reason: reason}
	}
}

// aliasTable maps active alias ids to their channels. It is sharded, as
// every call adds and removes an entry.
type aliasTable struct {
	shards [64]aliasShard
}

type aliasShard struct {
	mu sync.Mutex
	m  map[uint64]chan AliasMsg
	_  [48]byte // keep shards on separate cache lines
}

func (t *aliasTable) shard(id uint64) *aliasShard { return &t.shards[id%uint64(len(t.shards))] }

func (t *aliasTable) put(id uint64, ch chan AliasMsg) {
	s := t.shard(id)
	s.mu.Lock()
	if s.m == nil {
		s.m = make(map[uint64]chan AliasMsg)
	}
	s.m[id] = ch
	s.mu.Unlock()
}

// take removes the alias and returns its channel, so that only one
// message is ever delivered to it.
func (t *aliasTable) take(id uint64) (chan AliasMsg, bool) {
	s := t.shard(id)
	s.mu.Lock()
	ch, ok := s.m[id]
	delete(s.m, id)
	s.mu.Unlock()
	return ch, ok
}

func (t *aliasTable) del(id uint64) {
	s := t.shard(id)
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}

func (t *aliasTable) len() int {
	n := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.Lock()
		n += len(s.m)
		s.mu.Unlock()
	}
	return n
}
