package genstatem_test

import (
	"context"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

// door is locked or open, and takes requests bound to it: a machine is
// called through an actor.Ref as a genserver is.
type door struct{}

type (
	unlock   struct{ actor.Req[door, bool] }
	doorName struct{ actor.Req[door, string] }
)

func (door) Init(proc.PID) (string, struct{}, []actor.Effect, error) {
	return "locked", struct{}{}, nil, nil
}

func (door) HandleEvent(st string, d struct{}, ev genstatem.Event) (string, struct{}, []actor.Effect) {
	c, ok := ev.(genstatem.Call)
	if !ok {
		return st, d, nil
	}
	switch r := c.Req.(type) {
	case unlock:
		return "open", d, actor.Do(r.Reply(c.From, st == "locked"))
	case doorName:
		return st, d, actor.Do(r.Reply(c.From, "front"))
	}
	return st, d, actor.Do(actor.Unknown(c.From, c.Req))
}

func TestRefCallMachine(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	pid, err := genstatem.Start(ctx, n, door{})
	if err != nil {
		t.Fatal(err)
	}
	ref := actor.NewRef[door](pid)
	if was, err := ref.Call(ctx, n, unlock{}); !was || err != nil {
		t.Errorf("unlock = %v, %v", was, err)
	}
	if was, err := ref.Call(ctx, n, unlock{}); was || err != nil {
		t.Errorf("unlock again = %v, %v", was, err)
	}
	if name, err := ref.Call(ctx, n, doorName{}); name != "front" || err != nil {
		t.Errorf("doorName = %q, %v", name, err)
	}
}
