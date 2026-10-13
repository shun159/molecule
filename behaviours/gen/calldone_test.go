package gen_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/gensim"
	"github.com/shun159/molecule/proc"
)

// Call and Request tell, done, how long they took, to a handler of events.
func TestCallDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dones := make(chan actor.CallDone, 4)
		n := proc.NewNode("", proc.WithEvents(func(e any) {
			if d, ok := e.(actor.CallDone); ok {
				dones <- d
			}
		}))
		ctx := context.Background()
		pid, calls := holder(t, n)
		defer n.Spawn(func(s *proc.Self) error { s.Exit(pid, proc.Kill); return nil })

		go actor.Call(ctx, n, pid, "q")
		from := <-calls
		time.Sleep(2 * time.Second)
		actor.SendReply(n, from, 1)
		if d := <-dones; d != (actor.CallDone{To: pid, Req: "q", Took: 2 * time.Second}) {
			t.Errorf("Call: %+v", d)
		}

		p := actor.Request[int](n, pid, "r")
		from = <-calls
		time.Sleep(time.Second)
		actor.SendReply(n, from, 1)
		<-p.Done()
		if d := <-dones; d != (actor.CallDone{To: pid, Req: "r", Took: time.Second}) {
			t.Errorf("Request: %+v", d)
		}

		actor.Call(ctx, n, actor.Local("none"), "x")
		if d := <-dones; !errors.Is(d.Err, proc.NoProc) || d.Req != "x" {
			t.Errorf("Call to none: %+v", d)
		}
	})
}

// slow replies to each call 3 seconds after it.
type slow struct {
	genserver.Default[[]genserver.From[string]]
}

type replyNow struct{}

func (slow) HandleCall(fs []genserver.From[string], _ string, from genserver.From[string]) ([]genserver.From[string], []actor.Effect) {
	return append(fs[:len(fs):len(fs)], from), actor.Do(actor.StartTimer{Key: len(fs), After: 3 * time.Second, Msg: replyNow{}})
}

func (slow) HandleInfo(fs []genserver.From[string], _ any) ([]genserver.From[string], []actor.Effect) {
	if len(fs) == 0 {
		return fs, nil
	}
	return fs[1:], actor.Do(fs[0].Reply("done"))
}

// asker makes the SendRequest it is cast.
type asker struct{ genserver.Default[struct{}] }

func (asker) HandleCast(s struct{}, r actor.SendRequest) (struct{}, []actor.Effect) {
	return s, actor.Do(r)
}

// The SendRequest of a behaviour tells as well, timed by the clock of the
// simulation.
func TestCallDoneSimulated(t *testing.T) {
	var dones []actor.CallDone
	s := gensim.New(1, gensim.WithEvents(func(e any) {
		if d, ok := e.(actor.CallDone); ok {
			dones = append(dones, d)
		}
	}))
	server, err := gensim.Spawn(s, genserver.Gen(slow{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := gensim.Spawn(s, genserver.Gen(asker{}), nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Cast(r, actor.SendRequest{To: server, Req: "a", Tag: 1})
	s.Advance(10 * time.Second)
	s.Cast(r, actor.SendRequest{To: server, Req: "b", Tag: 2, Timeout: time.Second})
	s.Advance(10 * time.Second)
	want := []actor.CallDone{
		{To: server, Req: "a", Took: 3 * time.Second},
		{To: server, Req: "b", Took: time.Second, Err: context.DeadlineExceeded},
	}
	if len(dones) != 2 || dones[0] != want[0] || dones[1] != want[1] {
		t.Errorf("events %+v, want %+v", dones, want)
	}
}
