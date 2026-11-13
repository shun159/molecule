package genserver_test

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// calc takes requests bound to it, each replied a type of its own.
type calc struct{ genserver.Default[int] }

type (
	getN struct{ actor.Req[calc, int] }
	divN struct {
		actor.Req[calc, int]
		By int
	}
	describe struct{ actor.Req[calc, string] }
	// forgotten is a request to calc that calc does not handle.
	forgotten struct{ actor.Req[calc, int] }
)

func (calc) Init(proc.PID) (int, []actor.Effect, error) { return 12, nil, nil }

func (calc) HandleCall(n int, req actor.Of[calc], from genserver.From[any]) (int, []actor.Effect) {
	switch r := req.(type) {
	case getN:
		return n, actor.Do(r.Reply(from, n))
	case divN:
		n /= r.By
		return n, actor.Do(r.Reply(from, n))
	case describe:
		return n, actor.Do(r.Reply(from, "calc"))
	}
	return n, actor.Do(actor.Unknown(from, req))
}

func TestRefCall(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	started, err := genserver.Start(ctx, n, calc{})
	if err != nil {
		t.Fatal(err)
	}
	defer kill(n, started.Dest().(proc.PID))
	ref := actor.NewRef[calc](started.Dest())

	if v, err := ref.Call(ctx, n, divN{By: 3}); v != 4 || err != nil {
		t.Errorf("divN = %d, %v", v, err)
	}
	if v, err := ref.Call(ctx, n, describe{}); v != "calc" || err != nil {
		t.Errorf("describe = %q, %v", v, err)
	}
	var unknown *actor.UnknownRequestError
	if v, err := ref.Call(ctx, n, forgotten{}); v != 0 || !errors.As(err, &unknown) {
		t.Errorf("forgotten = %d, %v; want an UnknownRequestError", v, err)
	}
	if v, err := ref.Call(ctx, n, getN{}); v != 4 || err != nil {
		t.Errorf("getN after = %d, %v", v, err)
	}
}

// A request with Req is encoded by gob as without, to cross nodes.
func TestReqGob(t *testing.T) {
	gob.Register(divN{})
	var buf bytes.Buffer
	var in any = divN{By: 3}
	if err := gob.NewEncoder(&buf).Encode(&in); err != nil {
		t.Fatal(err)
	}
	var out any
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Errorf("decoded %#v, want %#v", out, in)
	}
}
