package pg

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// Scope is the behaviour of a scope. Name is the local name it is
// registered under, by which it finds the scopes of the same name on the
// other nodes; a scope without one keeps to its node.
type Scope struct{ Name string }

// Requests to a scope.
type (
	join struct {
		Group any
		PIDs  []proc.PID
	}
	leave struct {
		Group any
		PIDs  []proc.PID
	}
	send struct {
		Group  any
		Msg    any
		Except proc.PID
	}
)

// Queries to a scope, for molecule.SendRequest from a behaviour, or gensim.
type (
	// MembersRequest asks for the members of Group, as Members returns
	// them.
	MembersRequest struct{ Group any }
	// WhichRequest asks for the groups, as Which returns them.
	WhichRequest struct{}
)

// Messages between the scopes of a name on several nodes, as in Erlang's
// pg: a scope discovers another, which tells it its members, then what
// joins and leaves.
type (
	discover struct{ From proc.PID }
	synced   struct {
		From   proc.PID
		Groups []groupMembers
	}
	joined struct {
		From  proc.PID
		Group any
		PIDs  []proc.PID
	}
	left struct {
		From  proc.PID
		Group any
		PIDs  []proc.PID
	}
	groupMembers struct {
		Group any
		PIDs  []proc.PID
	}
	// peerTag tags the monitor of another scope.
	peerTag struct{ PID proc.PID }
)

func init() {
	dist.Register(join{}, leave{}, send{}, MembersRequest{}, WhichRequest{},
		discover{}, synced{}, joined{}, left{}, []proc.PID{}, []any{})
}

// state is the state of a scope: its members, joined through it, and those
// of the scopes of other nodes, by scope.
type state struct {
	self  proc.PID
	local groups
	peers map[proc.PID]groups
}

// groups are the members of each group, in the order they joined, a
// process once per join, and the groups in the order they were created.
type groups struct {
	members map[any][]proc.PID
	order   []any
}

// Start starts a scope registered as name. A molecule.Local name makes it
// share its groups with the scopes of the same name on other nodes.
func Start(ctx context.Context, n *proc.Node, name molecule.Name) (proc.PID, error) {
	ref, err := genserver.Start(ctx, n, scopeFor(name), molecule.WithName(name))
	return pidOf(ref), err
}

// StartLink starts a scope registered as name, linked to parent.
func StartLink(ctx context.Context, parent *proc.Self, name molecule.Name) (proc.PID, error) {
	ref, err := genserver.StartLink(ctx, parent, scopeFor(name), molecule.WithName(name))
	return pidOf(ref), err
}

// Child returns a scope registered as name as a child to start, the Start
// of a supervisor.ChildSpec.
func Child(name molecule.Name) gen.Child {
	return genserver.Child(scopeFor(name), molecule.WithName(name))
}

func scopeFor(name molecule.Name) Scope {
	l, _ := name.(molecule.Local)
	return Scope{Name: string(l)}
}

func pidOf(ref genserver.Ref[any, any, any]) proc.PID {
	pid, _ := ref.Dest().(proc.PID)
	return pid
}

// Join makes pids members of group in the scope, once more each.
func Join(ctx context.Context, caller molecule.Caller, scope molecule.Dest, group any, pids ...proc.PID) error {
	_, err := molecule.Call(ctx, caller, scope, join{group, pids})
	return err
}

// Leave makes pids members of group once less each. Leaving a group one
// is not in does nothing.
func Leave(ctx context.Context, caller molecule.Caller, scope molecule.Dest, group any, pids ...proc.PID) error {
	_, err := molecule.Call(ctx, caller, scope, leave{group, pids})
	return err
}

// Members returns the members of group: those joined through the scope,
// in the order they joined, a process once per join, then those of the
// scopes of other nodes.
func Members(ctx context.Context, caller molecule.Caller, scope molecule.Dest, group any) ([]proc.PID, error) {
	v, err := molecule.Call(ctx, caller, scope, MembersRequest{group})
	pids, _ := v.([]proc.PID)
	return pids, err
}

// Which returns the groups having members, on any node.
func Which(ctx context.Context, caller molecule.Caller, scope molecule.Dest) ([]any, error) {
	v, err := molecule.Call(ctx, caller, scope, WhichRequest{})
	groups, _ := v.([]any)
	return groups, err
}

// JoinEffect is the effect making pids members of group, for a behaviour.
func JoinEffect(scope molecule.Dest, group any, pids ...proc.PID) molecule.Effect {
	return molecule.Cast{To: scope, Req: join{group, pids}}
}

// LeaveEffect is the effect making pids leave group, for a behaviour.
func LeaveEffect(scope molecule.Dest, group any, pids ...proc.PID) molecule.Effect {
	return molecule.Cast{To: scope, Req: leave{group, pids}}
}

// SendEffect is the effect having the scope send msg to every member of
// group, on any node, once per member however many times it joined, except
// the process except, which may be zero.
func SendEffect(scope molecule.Dest, group any, msg any, except proc.PID) molecule.Effect {
	return molecule.Cast{To: scope, Req: send{group, msg, except}}
}

func (sc Scope) Init(self proc.PID) (state, []molecule.Effect, error) {
	s := state{self: self, local: groups{members: map[any][]proc.PID{}}, peers: map[proc.PID]groups{}}
	if sc.Name == "" {
		return s, nil, nil
	}
	return s, molecule.Do(molecule.MonitorNodes{On: true}), nil
}

func (sc Scope) HandleCall(s state, req any, from genserver.From[any]) (state, []molecule.Effect) {
	switch r := req.(type) {
	case MembersRequest:
		return s, molecule.Do(from.Reply(s.members(r.Group)))
	case WhichRequest:
		return s, molecule.Do(from.Reply(s.which()))
	}
	s, effs := sc.HandleCast(s, req)
	return s, append(effs, from.Reply(nil))
}

func (Scope) HandleCast(s state, req any) (state, []molecule.Effect) {
	switch r := req.(type) {
	case join:
		return s.join(r.Group, r.PIDs)
	case leave:
		return s.leave(r.Group, r.PIDs)
	case send:
		var effs []molecule.Effect
		seen := map[proc.PID]bool{r.Except: true}
		for _, pid := range s.members(r.Group) {
			if !seen[pid] {
				seen[pid] = true
				effs = append(effs, molecule.Send{To: pid, Msg: r.Msg})
			}
		}
		return s, effs
	}
	return s, nil
}

// HandleInfo keeps up with the other scopes, and takes the members that
// exit out of all their groups.
func (sc Scope) HandleInfo(s state, msg any) (state, []molecule.Effect) {
	switch m := msg.(type) {
	case proc.NodeUp:
		return s, molecule.Do(molecule.Send{To: molecule.Local(sc.Name).At(m.Node), Msg: discover{s.self}})
	case discover:
		s, effs := s.meet(m.From, true)
		return s, append(effs, molecule.Send{To: m.From, Msg: synced{s.self, s.local.all()}})
	case synced:
		s, effs := s.meet(m.From, false)
		g := groups{members: map[any][]proc.PID{}}
		for _, gm := range m.Groups {
			g = g.add(gm.Group, gm.PIDs)
		}
		s.peers = maps.Clone(s.peers)
		s.peers[m.From] = g
		return s, effs
	case joined:
		if g, ok := s.peers[m.From]; ok {
			s.peers = maps.Clone(s.peers)
			s.peers[m.From] = g.add(m.Group, m.PIDs)
		}
	case left:
		if g, ok := s.peers[m.From]; ok {
			s.peers = maps.Clone(s.peers)
			s.peers[m.From], _ = g.remove(m.Group, m.PIDs)
		}
	case molecule.Down:
		if p, ok := m.Tag.(peerTag); ok {
			s.peers = maps.Clone(s.peers)
			delete(s.peers, p.PID)
			return s, nil
		}
		return s.exited(m.PID)
	}
	return s, nil
}

// meet takes the scope peer, of another node, if new: it watches it, and
// tells it about itself if asked to.
func (s state) meet(peer proc.PID, discoverBack bool) (state, []molecule.Effect) {
	if _, ok := s.peers[peer]; ok {
		return s, nil
	}
	s.peers = maps.Clone(s.peers)
	s.peers[peer] = groups{members: map[any][]proc.PID{}}
	effs := []molecule.Effect{molecule.Monitor{Target: peer, Tag: peerTag{peer}}}
	if discoverBack {
		effs = append(effs, molecule.Send{To: peer, Msg: discover{s.self}})
	}
	return s, effs
}

func (s state) join(group any, pids []proc.PID) (state, []molecule.Effect) {
	var effs []molecule.Effect
	for _, pid := range pids {
		if s.joins(pid) == 0 {
			effs = append(effs, molecule.Monitor{Target: pid, Tag: pid})
		}
		s.local = s.local.add(group, []proc.PID{pid})
	}
	return s, append(effs, s.tellPeers(joined{s.self, group, pids})...)
}

func (s state) leave(group any, pids []proc.PID) (state, []molecule.Effect) {
	var gone []proc.PID
	s.local, gone = s.local.remove(group, pids)
	var effs []molecule.Effect
	seen := map[proc.PID]bool{}
	for _, pid := range gone {
		if !seen[pid] && s.joins(pid) == 0 {
			effs = append(effs, molecule.Demonitor{Tag: pid})
		}
		seen[pid] = true
	}
	if len(gone) > 0 {
		effs = append(effs, s.tellPeers(left{s.self, group, gone})...)
	}
	return s, effs
}

// exited takes pid, exited, out of its groups, as many times as it joined.
func (s state) exited(pid proc.PID) (state, []molecule.Effect) {
	var effs []molecule.Effect
	for _, group := range s.local.order {
		var times []proc.PID
		for _, p := range s.local.members[group] {
			if p == pid {
				times = append(times, p)
			}
		}
		if len(times) == 0 {
			continue
		}
		s.local, _ = s.local.remove(group, times)
		effs = append(effs, s.tellPeers(left{s.self, group, times})...)
	}
	return s, effs
}

// joins returns how many times pid is a member, through this scope.
func (s state) joins(pid proc.PID) int {
	n := 0
	for _, m := range s.local.members {
		for _, p := range m {
			if p == pid {
				n++
			}
		}
	}
	return n
}

// tellPeers sends msg to the scopes of the other nodes.
func (s state) tellPeers(msg any) []molecule.Effect {
	var effs []molecule.Effect
	for _, peer := range s.peerOrder() {
		effs = append(effs, molecule.Send{To: peer, Msg: msg})
	}
	return effs
}

// peerOrder returns the other scopes, in an order of their own.
func (s state) peerOrder() []proc.PID {
	return slices.SortedFunc(maps.Keys(s.peers), func(a, b proc.PID) int { return cmp.Compare(a.String(), b.String()) })
}

func (s state) members(group any) []proc.PID {
	pids := slices.Clone(s.local.members[group])
	for _, peer := range s.peerOrder() {
		pids = append(pids, s.peers[peer].members[group]...)
	}
	return pids
}

func (s state) which() []any {
	out := slices.Clone(s.local.order)
	for _, peer := range s.peerOrder() {
		for _, g := range s.peers[peer].order {
			if !slices.Contains(out, g) {
				out = append(out, g)
			}
		}
	}
	return out
}

// add returns g with pids members of group once more each; g is not
// changed.
func (g groups) add(group any, pids []proc.PID) groups {
	if len(pids) == 0 {
		return g
	}
	next := groups{members: maps.Clone(g.members), order: slices.Clip(g.order)}
	if next.members == nil {
		next.members = map[any][]proc.PID{}
	}
	if _, ok := next.members[group]; !ok {
		next.order = append(next.order, group)
	}
	next.members[group] = append(slices.Clip(next.members[group]), pids...)
	return next
}

// remove returns g with pids members of group once less each, and those
// that were; g is not changed. A group left empty is forgotten.
func (g groups) remove(group any, pids []proc.PID) (groups, []proc.PID) {
	members := slices.Clone(g.members[group])
	var gone []proc.PID
	for _, pid := range pids {
		if i := slices.Index(members, pid); i >= 0 {
			members = slices.Delete(members, i, i+1)
			gone = append(gone, pid)
		}
	}
	if len(gone) == 0 {
		return g, nil
	}
	next := groups{members: maps.Clone(g.members), order: slices.Clone(g.order)}
	if len(members) == 0 {
		delete(next.members, group)
		next.order = slices.DeleteFunc(next.order, func(x any) bool { return x == group })
	} else {
		next.members[group] = members
	}
	return next, gone
}

// all returns the members of all groups, for another scope.
func (g groups) all() []groupMembers {
	var out []groupMembers
	for _, group := range g.order {
		out = append(out, groupMembers{group, slices.Clone(g.members[group])})
	}
	return out
}
