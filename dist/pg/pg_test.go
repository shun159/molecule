package pg_test

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/dist/pg"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// inbox is a server logging the plain messages it gets.
type inbox struct{}

func (inbox) Init(proc.PID) ([]any, []molecule.Effect, error) { return nil, nil, nil }
func (inbox) HandleCall(l []any, _ struct{}, _ genserver.From[struct{}]) ([]any, []molecule.Effect) {
	return l, nil
}

// HandleCast runs the effects it is cast, as a behaviour would return.
func (inbox) HandleCast(l []any, effs []molecule.Effect) ([]any, []molecule.Effect) { return l, effs }

func (inbox) HandleInfo(l []any, msg any) ([]any, []molecule.Effect) {
	return append(slices.Clip(l), msg), nil
}

// sim is a simulation with a scope "pg" and three inboxes.
func sim(t *testing.T, seed uint64) (*gensim.Sim, molecule.Local, []proc.PID) {
	t.Helper()
	s := gensim.New(seed)
	if _, err := gensim.Spawn(s, genserver.Gen(pg.Scope{}), nil, gensim.Named("pg")); err != nil {
		t.Fatal(err)
	}
	var pids []proc.PID
	for range 3 {
		pid, err := gensim.Spawn(s, genserver.Gen(inbox{}), nil)
		if err != nil {
			t.Fatal(err)
		}
		pids = append(pids, pid)
	}
	return s, molecule.Local("pg"), pids
}

func call[T any](t *testing.T, s *gensim.Sim, scope molecule.Dest, req any) T {
	t.Helper()
	v, err := s.Call(scope, req)
	if err != nil {
		t.Fatal(err)
	}
	r, _ := v.(T)
	return r
}

// The requests are those the client functions send; casting effects to an
// inbox has it run them, as a behaviour returning them.
func run(s *gensim.Sim, pid proc.PID, effs ...molecule.Effect) {
	s.Cast(pid, effs)
	s.RunUntilIdle()
}

func members(t *testing.T, s *gensim.Sim, scope molecule.Dest, group any) []proc.PID {
	t.Helper()
	return call[[]proc.PID](t, s, scope, pg.MembersRequest{Group: group})
}

func which(t *testing.T, s *gensim.Sim, scope molecule.Dest) []any {
	t.Helper()
	return call[[]any](t, s, scope, pg.WhichRequest{})
}

func TestJoinLeave(t *testing.T) {
	s, scope, p := sim(t, 1)
	run(s, p[0], pg.JoinEffect(scope, "room", p[0], p[1]))
	run(s, p[0], pg.JoinEffect(scope, "room", p[0])) // twice
	run(s, p[2], pg.JoinEffect(scope, "other", p[2]))

	if got := members(t, s, scope, "room"); !reflect.DeepEqual(got, []proc.PID{p[0], p[1], p[0]}) {
		t.Errorf("members = %v", got)
	}
	if got := which(t, s, scope); !reflect.DeepEqual(got, []any{"room", "other"}) {
		t.Errorf("which = %v", got)
	}

	run(s, p[0], pg.LeaveEffect(scope, "room", p[0]))
	if got := members(t, s, scope, "room"); !reflect.DeepEqual(got, []proc.PID{p[1], p[0]}) {
		t.Errorf("after one leave: %v", got)
	}
	run(s, p[0], pg.LeaveEffect(scope, "room", p[0], p[1]), pg.LeaveEffect(scope, "nowhere", p[1]))
	if got := which(t, s, scope); !reflect.DeepEqual(got, []any{"other"}) {
		t.Errorf("empty group kept: %v", got)
	}
}

func TestMemberExit(t *testing.T) {
	s, scope, p := sim(t, 1)
	run(s, p[0], pg.JoinEffect(scope, "a", p[0], p[1]), pg.JoinEffect(scope, "b", p[1]))
	s.Exit(p[1], proc.Kill)
	s.RunUntilIdle()
	if got := members(t, s, scope, "a"); !reflect.DeepEqual(got, []proc.PID{p[0]}) {
		t.Errorf("members of a = %v", got)
	}
	if got := which(t, s, scope); !reflect.DeepEqual(got, []any{"a"}) {
		t.Errorf("which = %v", got)
	}
}

func TestSendEffect(t *testing.T) {
	for seed := range uint64(20) {
		s, scope, p := sim(t, seed)
		run(s, p[0], pg.JoinEffect(scope, "room", p[0], p[1], p[2], p[1]))
		run(s, p[0], pg.SendEffect(scope, "room", "hello", p[0]))
		for i, want := range [][]any{nil, {"hello"}, {"hello"}} {
			if got, _ := gensim.State[[]any](s, p[i]); !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d: inbox %d got %v, want %v", seed, i, got, want)
			}
		}
	}
}

// TestRealProcesses runs a scope in real processes, through the client
// functions.
func TestRealProcesses(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	if _, err := pg.Start(ctx, n, molecule.Local("pg")); err != nil {
		t.Fatal(err)
	}
	a := n.Spawn(func(s *proc.Self) error { _, err := s.Receive(ctx); return err })
	b := n.Spawn(func(s *proc.Self) error { _, err := s.Receive(ctx); return err })
	if err := pg.Join(ctx, n, molecule.Local("pg"), "g", a, b); err != nil {
		t.Fatal(err)
	}
	got, err := pg.Members(ctx, n, molecule.Local("pg"), "g")
	if err != nil || !reflect.DeepEqual(got, []proc.PID{a, b}) {
		t.Errorf("Members = %v, %v", got, err)
	}
	if err := pg.Leave(ctx, n, molecule.Local("pg"), "g", a); err != nil {
		t.Fatal(err)
	}
	groups, err := pg.Which(ctx, n, molecule.Local("pg"))
	got, _ = pg.Members(ctx, n, molecule.Local("pg"), "g")
	if err != nil || !reflect.DeepEqual(groups, []any{"g"}) || !reflect.DeepEqual(got, []proc.PID{b}) {
		t.Errorf("after Leave: %v %v, %v", groups, got, err)
	}
}
