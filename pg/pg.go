package pg

import (
	"context"
	"slices"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// Scope is the behaviour of a scope.
type Scope struct{}

// Requests to a scope.
type (
	join struct {
		group any
		pids  []proc.PID
	}
	leave struct {
		group any
		pids  []proc.PID
	}
	send struct {
		group  any
		msg    any
		except proc.PID
	}
)

// Queries to a scope, for gen.SendRequest from a behaviour, or gensim.
type (
	// MembersRequest asks for the members of Group, as Members returns
	// them.
	MembersRequest struct{ Group any }
	// WhichRequest asks for the groups, as Which returns them.
	WhichRequest struct{}
)

// state is the state of a scope: the members of each group, in the order
// they joined, a process once per join.
type state struct {
	groups map[any][]proc.PID
	order  []any // the groups, in the order they were created
	joins  map[proc.PID]int
}

// Start starts a scope registered as name.
func Start(ctx context.Context, n *proc.Node, name gen.Name) (proc.PID, error) {
	ref, err := genserver.Start(ctx, n, Scope{}, gen.WithName(name))
	return pidOf(ref), err
}

// StartLink starts a scope registered as name, linked to parent.
func StartLink(ctx context.Context, parent *proc.Self, name gen.Name) (proc.PID, error) {
	ref, err := genserver.StartLink(ctx, parent, Scope{}, gen.WithName(name))
	return pidOf(ref), err
}

// StartLinkFunc returns the start function of a scope registered as name,
// for a supervisor.ChildSpec.
func StartLinkFunc(name gen.Name) func(context.Context, *proc.Self) (proc.PID, error) {
	return genserver.StartLinkFunc(Scope{}, gen.WithName(name))
}

func pidOf(ref genserver.Ref[any, any, any]) proc.PID {
	pid, _ := ref.Dest().(proc.PID)
	return pid
}

// Join makes pids members of group in the scope, once more each.
func Join(ctx context.Context, caller gen.Caller, scope gen.Dest, group any, pids ...proc.PID) error {
	_, err := gen.Call(ctx, caller, scope, join{group, pids})
	return err
}

// Leave makes pids members of group once less each. Leaving a group one
// is not in does nothing.
func Leave(ctx context.Context, caller gen.Caller, scope gen.Dest, group any, pids ...proc.PID) error {
	_, err := gen.Call(ctx, caller, scope, leave{group, pids})
	return err
}

// Members returns the members of group, in the order they joined, a
// process once per join.
func Members(ctx context.Context, caller gen.Caller, scope gen.Dest, group any) ([]proc.PID, error) {
	v, err := gen.Call(ctx, caller, scope, MembersRequest{group})
	pids, _ := v.([]proc.PID)
	return pids, err
}

// Which returns the groups having members, in the order they were created.
func Which(ctx context.Context, caller gen.Caller, scope gen.Dest) ([]any, error) {
	v, err := gen.Call(ctx, caller, scope, WhichRequest{})
	groups, _ := v.([]any)
	return groups, err
}

// JoinEffect is the effect making pids members of group, for a behaviour.
func JoinEffect(scope gen.Dest, group any, pids ...proc.PID) gen.Effect {
	return gen.Cast{To: scope, Req: join{group, pids}}
}

// LeaveEffect is the effect making pids leave group, for a behaviour.
func LeaveEffect(scope gen.Dest, group any, pids ...proc.PID) gen.Effect {
	return gen.Cast{To: scope, Req: leave{group, pids}}
}

// SendEffect is the effect having the scope send msg to every member of
// group, once per member however many times it joined, except the process
// except, which may be zero.
func SendEffect(scope gen.Dest, group any, msg any, except proc.PID) gen.Effect {
	return gen.Cast{To: scope, Req: send{group, msg, except}}
}

func (Scope) Init(proc.PID) (state, []gen.Effect, error) {
	return state{groups: map[any][]proc.PID{}, joins: map[proc.PID]int{}}, nil, nil
}

func (sc Scope) HandleCall(s state, req any, from genserver.From[any]) (state, []gen.Effect) {
	switch r := req.(type) {
	case MembersRequest:
		return s, gen.Do(from.Reply(slices.Clone(s.groups[r.Group])))
	case WhichRequest:
		return s, gen.Do(from.Reply(slices.Clone(s.order)))
	}
	s, effs := sc.HandleCast(s, req)
	return s, append(effs, from.Reply(nil))
}

func (Scope) HandleCast(s state, req any) (state, []gen.Effect) {
	switch r := req.(type) {
	case join:
		return s.join(r.group, r.pids)
	case leave:
		return s.leave(r.group, r.pids)
	case send:
		var effs []gen.Effect
		seen := map[proc.PID]bool{r.except: true}
		for _, pid := range s.groups[r.group] {
			if !seen[pid] {
				seen[pid] = true
				effs = append(effs, gen.Send{To: pid, Msg: r.msg})
			}
		}
		return s, effs
	}
	return s, nil
}

// HandleInfo takes the members that exit out of all their groups.
func (Scope) HandleInfo(s state, msg any) (state, []gen.Effect) {
	down, ok := msg.(gen.Down)
	if !ok {
		return s, nil
	}
	pid := down.PID
	next := s.clone()
	for _, group := range s.order {
		next.groups[group] = slices.DeleteFunc(next.groups[group], func(p proc.PID) bool { return p == pid })
	}
	delete(next.joins, pid)
	return next.prune(), nil
}

func (s state) join(group any, pids []proc.PID) (state, []gen.Effect) {
	next := s.clone()
	var effs []gen.Effect
	if _, ok := next.groups[group]; !ok {
		next.order = append(next.order, group)
	}
	for _, pid := range pids {
		next.groups[group] = append(next.groups[group], pid)
		if next.joins[pid] == 0 {
			effs = append(effs, gen.Monitor{Target: pid, Tag: pid})
		}
		next.joins[pid]++
	}
	return next, effs
}

func (s state) leave(group any, pids []proc.PID) (state, []gen.Effect) {
	next := s.clone()
	var effs []gen.Effect
	for _, pid := range pids {
		members := next.groups[group]
		i := slices.Index(members, pid)
		if i < 0 {
			continue
		}
		next.groups[group] = slices.Delete(members, i, i+1)
		if next.joins[pid]--; next.joins[pid] == 0 {
			delete(next.joins, pid)
			effs = append(effs, gen.Demonitor{Tag: pid})
		}
	}
	return next.prune(), effs
}

// clone copies s, so that the state a callback got is never modified.
func (s state) clone() state {
	next := state{groups: make(map[any][]proc.PID, len(s.groups)), order: slices.Clone(s.order), joins: make(map[proc.PID]int, len(s.joins))}
	for g, m := range s.groups {
		next.groups[g] = slices.Clone(m)
	}
	for p, n := range s.joins {
		next.joins[p] = n
	}
	return next
}

// prune forgets the groups left empty.
func (s state) prune() state {
	s.order = slices.DeleteFunc(s.order, func(g any) bool {
		if len(s.groups[g]) == 0 {
			delete(s.groups, g)
			return true
		}
		return false
	})
	return s
}
