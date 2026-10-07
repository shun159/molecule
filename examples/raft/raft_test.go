package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
)

// The rules of figure 2, tested as calls of a pure function.

var three = Server{
	Node:            "n1",
	Peers:           []string{"n2", "n3"},
	ElectionTimeout: 150 * time.Millisecond,
	Heartbeat:       50 * time.Millisecond,
}

func cast(role Role, d Data, msg any) (Role, Data, []gen.Effect) {
	return three.HandleEvent(role, d, genstatem.Cast{Msg: msg})
}

// sent returns the casts among effs, by node.
func sent(effs []gen.Effect) map[string]any {
	out := map[string]any{}
	for _, e := range effs {
		if c, ok := e.(gen.Cast); ok {
			out[c.To.(gen.Remote).Node] = c.Req
		}
	}
	return out
}

func TestOneVotePerTerm(t *testing.T) {
	d := Data{Persistent: Persistent{Term: 1}}
	_, d, effs := cast(Follower, d, RequestVote{Term: 1, Candidate: "n2"})
	if v := sent(effs)["n2"]; v != (Vote{Term: 1, From: "n1", Granted: true}) || d.VotedFor != "n2" {
		t.Errorf("first vote: %#v, voted for %q", v, d.VotedFor)
	}
	_, _, effs = cast(Follower, d, RequestVote{Term: 1, Candidate: "n3"})
	if v := sent(effs)["n3"]; v != (Vote{Term: 1, From: "n1"}) {
		t.Errorf("second vote in the term: %#v", v)
	}
	// A newer term frees the vote.
	_, d, effs = cast(Follower, d, RequestVote{Term: 2, Candidate: "n3"})
	if v := sent(effs)["n3"]; v != (Vote{Term: 2, From: "n1", Granted: true}) || d.Term != 2 {
		t.Errorf("vote in a newer term: %#v, term %d", v, d.Term)
	}
}

func TestVoteForUpToDateLogs(t *testing.T) {
	d := Data{Persistent: Persistent{Term: 3, Log: []Entry{{1, "a"}, {3, "b"}}}}
	for _, tt := range []struct {
		lastIndex int
		lastTerm  uint64
		grant     bool
	}{
		{2, 3, true},  // as long
		{5, 3, true},  // longer
		{1, 3, false}, // the same last term, shorter: behind
		{1, 4, true},  // shorter, but a newer last term
		{9, 2, false}, // longer, but an older last term
	} {
		_, _, effs := cast(Follower, d, RequestVote{Term: 3, Candidate: "n2", LastIndex: tt.lastIndex, LastTerm: tt.lastTerm})
		if v := sent(effs)["n2"].(Vote); v.Granted != tt.grant {
			t.Errorf("last %d of term %d: granted %v", tt.lastIndex, tt.lastTerm, v.Granted)
		}
	}
}

func TestAppendEntries(t *testing.T) {
	d := Data{Persistent: Persistent{Term: 2, Log: []Entry{{1, "a"}, {1, "b"}, {1, "stale"}}}}
	// A conflict at 3 drops it, and the entries of the leader follow.
	role, d, effs := cast(Candidate, d, AppendEntries{
		Term: 2, Leader: "n2", PrevIndex: 2, PrevTerm: 1,
		Entries: []Entry{{2, "c"}, {2, "d"}}, Commit: 3,
	})
	want := []Entry{{1, "a"}, {1, "b"}, {2, "c"}, {2, "d"}}
	if role != Follower || !reflect.DeepEqual(d.Log, want) || d.Commit != 3 || d.Leader != "n2" {
		t.Errorf("%v %+v", role, d)
	}
	if r := sent(effs)["n2"]; r != (Appended{Term: 2, From: "n1", Success: true, Match: 4}) {
		t.Errorf("reply %#v", r)
	}
	// What does not follow the log is refused.
	_, d2, effs := cast(Follower, d, AppendEntries{Term: 2, Leader: "n2", PrevIndex: 4, PrevTerm: 1})
	if r := sent(effs)["n2"]; r != (Appended{Term: 2, From: "n1"}) || !reflect.DeepEqual(d2.Log, want) {
		t.Errorf("mismatch: %#v, log %v", r, d2.Log)
	}
	// A stale leader is refused, and nothing changes.
	_, d3, effs := cast(Follower, d, AppendEntries{Term: 1, Leader: "n3", PrevIndex: 0})
	if r := sent(effs)["n3"]; r != (Appended{Term: 2, From: "n1"}) || d3.Leader != "n2" {
		t.Errorf("stale leader: %#v, leader %q", r, d3.Leader)
	}
}

func TestLeaderStepsDown(t *testing.T) {
	d := Data{Persistent: Persistent{Term: 4}, Leader: "n1"}
	role, d, _ := cast(Leader, d, Appended{Term: 5, From: "n2"})
	if role != Follower || d.Term != 5 || d.Leader != "" {
		t.Errorf("%v %+v", role, d)
	}
}

func TestCommitOnlyCurrentTerm(t *testing.T) {
	// The leader of term 3 has an entry of term 2 on a majority: it is not
	// committed by counting (figure 8), until an entry of term 3 is.
	d := Data{
		Persistent: Persistent{Term: 3, Log: []Entry{{2, "old"}}},
		next:       map[string]int{"n2": 2, "n3": 1},
		match:      map[string]int{"n3": 0},
	}
	_, d, _ = cast(Leader, d, Appended{Term: 3, From: "n2", Success: true, Match: 1})
	if d.Commit != 0 {
		t.Errorf("entry of term 2 committed by counting")
	}
	_, d, _ = cast(Leader, d, Propose{"new"})
	_, d, _ = cast(Leader, d, Appended{Term: 3, From: "n2", Success: true, Match: 2})
	if d.Commit != 2 {
		t.Errorf("commit %d, want 2", d.Commit)
	}
}
