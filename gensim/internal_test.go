package gensim

import (
	"testing"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// echo replies to calls with their request, and on a cast starts then
// cancels a timer, and requests from itself.
type echo struct{}

func (echo) Init(proc.PID, any) (proc.PID, []actor.Effect, error) { return proc.PID{}, nil, nil }

func (echo) Handle(self proc.PID, msg gen.Msg) (proc.PID, []actor.Effect) {
	switch m := msg.(type) {
	case actor.CallMsg:
		return self, actor.Do(actor.Reply{To: m.From, Value: m.Req})
	case actor.CastMsg:
		return self, actor.Do(
			actor.StartTimer{Key: "t", After: time.Second, Msg: "tick"},
			actor.CancelTimer{Key: "t"},
			actor.SendRequest{To: m.Req.(proc.PID), Req: "ping", Tag: "r"},
		)
	}
	return self, nil
}

func (echo) Terminate(proc.PID, error) []actor.Effect { return nil }

// TestNoLeftovers checks that answered aliases and cancelled timers leave
// nothing behind: no alias kept, no timer firing, no extra message.
func TestNoLeftovers(t *testing.T) {
	s := New(1)
	a, _ := Spawn(s, gen.Behaviour[proc.PID](echo{}), nil)
	b, _ := Spawn(s, gen.Behaviour[proc.PID](echo{}), nil)

	if v, err := s.Call(a, "hi"); v != "hi" || err != nil {
		t.Fatalf("Call = %v, %v", v, err)
	}
	s.Cast(a, b) // a requests from b, which answers
	s.RunUntilIdle()
	s.Exit(b, proc.Kill) // must not answer the request again
	s.Advance(time.Minute)

	if len(s.aliases) != 0 {
		t.Errorf("%d aliases left", len(s.aliases))
	}
	sentToA := 0
	for _, e := range s.Trace() {
		if e.Kind == Fired {
			t.Errorf("a cancelled timer fired: %v", e)
		}
		if e.Kind == Sent && e.To == a && e.From == b {
			sentToA++
		}
	}
	if sentToA != 1 {
		t.Errorf("b sent a %d messages, want only the answer:\n%s", sentToA, s.TraceString())
	}
	if !s.Now().Equal(time.Date(2000, 1, 1, 0, 1, 0, 0, time.UTC)) {
		t.Errorf("clock at %v", s.Now())
	}
}

// A map whose entries share a pointer, with each other or with what comes
// after it, is fingerprinted alike every time, whatever order Go ranges
// over it in; a change of what they share still shows.
func TestFingerprintMapSharing(t *testing.T) {
	type holder struct{ N int }
	p := &holder{1}
	type state struct {
		M     map[string]*holder
		After *holder
	}
	s := state{M: map[string]*holder{"a": p, "b": p, "c": {2}, "d": {3}}, After: p}
	fp := fingerprint(s)
	for range 100 {
		if got := fingerprint(s); got != fp {
			t.Fatalf("one state fingerprinted %s, then %s", fp, got)
		}
	}
	p.N = 9
	if fingerprint(s) == fp {
		t.Error("a change of the pointer shared goes unseen")
	}
}
