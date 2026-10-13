package supervisor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// supEvents keeps the events of supervisors only.
type supEvents struct {
	mu sync.Mutex
	es []any
}

func (e *supEvents) add(ev any) {
	switch ev.(type) {
	case Restarted, GaveUp:
		e.mu.Lock()
		e.es = append(e.es, ev)
		e.mu.Unlock()
	}
}

func (e *supEvents) all() []any {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]any(nil), e.es...)
}

// A supervisor tells each restart, then giving up, as events.
func TestSimEvents(t *testing.T) {
	var evs supEvents
	s := gensim.New(1, gensim.WithEvents(evs.add))
	top, err := gensim.Start(s, Child(Spec{Intensity: 2, Period: 5 * time.Second, Children: []ChildSpec{
		worker("a", Permanent), worker("b", Permanent),
	}}))
	if err != nil {
		t.Fatal(err)
	}
	tr := &tree{t, s, top}
	var crashed []proc.PID
	for range 3 {
		b := tr.children(top)["b"]
		crashed = append(crashed, b)
		tr.crash(b)
	}
	got := evs.all()
	if len(got) != 3 {
		t.Fatalf("events %v, want 3", got)
	}
	for i, ev := range got {
		var sup, pid proc.PID
		var child string
		var reason error
		switch e := ev.(type) {
		case Restarted:
			if i == 2 {
				t.Errorf("event %d: %v, want GaveUp", i, e)
			}
			sup, child, pid, reason = e.Supervisor, e.Child, e.PID, e.Reason
		case GaveUp:
			if i != 2 {
				t.Errorf("event %d: %v, want Restarted", i, e)
			}
			sup, child, pid, reason = e.Supervisor, e.Child, e.PID, e.Reason
		}
		var pe *proc.PanicError
		if sup != top || child != "b" || pid != crashed[i] || !errors.As(reason, &pe) {
			t.Errorf("event %d: %#v", i, ev)
		}
	}
}

// In a process as well, the events reach the handler of the node.
func TestEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var evs supEvents
		n := proc.NewNode("", proc.WithEvents(evs.add), proc.WithLogger(slog.New(slog.DiscardHandler)))
		ctx := context.Background()
		sup, err := Start(ctx, n, Spec{Children: []ChildSpec{worker("b", Permanent)}})
		if err != nil {
			t.Fatal(err)
		}
		cs, err := WhichChildren(ctx, n, sup)
		if err != nil {
			t.Fatal(err)
		}
		b := cs[0].PID
		n.Send(b, actor.CastMsg{Req: crashNow{}})
		synctest.Wait()
		got := evs.all()
		if len(got) != 1 {
			t.Fatalf("events %v, want one", got)
		}
		if r, ok := got[0].(Restarted); !ok || r.Supervisor != sup || r.Child != "b" || r.PID != b {
			t.Errorf("event %#v", got[0])
		}
		Stop(ctx, n, sup)
	})
}
