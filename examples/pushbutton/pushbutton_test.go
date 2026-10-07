package main

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

func TestRun(t *testing.T) {
	var out bytes.Buffer
	if err := run(&out); err != nil {
		t.Fatal(err)
	}
	if want := "push: on\npush: off\npush: on\ncount: 2\n"; out.String() != want {
		t.Errorf("output:\n%s\nwant:\n%s", out.String(), want)
	}
}

// The state functions are pure: each is tested as a plain function.
func TestStateFunctions(t *testing.T) {
	from := gen.From{}
	call := func(req any) genstatem.Event { return genstatem.Call{From: from, Req: req} }
	reply := func(v any) []gen.Effect { return gen.Do(gen.Reply{To: from, Value: v}) }

	for _, tt := range []struct {
		state State
		ev    genstatem.Event
		next  State
		data  int
		effs  []gen.Effect
	}{
		{off{}, call(pushReq{}), on{}, 1, reply("on")},
		{on{}, call(pushReq{}), off{}, 0, reply("off")},
		{on{}, call(getCountReq{}), on{}, 0, reply(0)},
		{off{}, genstatem.Cast{Msg: "ignored"}, off{}, 0, nil},
	} {
		next, data, effs := Pushbutton{}.HandleEvent(tt.state, 0, tt.ev)
		if next != tt.next || data != tt.data || !reflect.DeepEqual(effs, tt.effs) {
			t.Errorf("%T on %#v: %T %d %#v", tt.state, tt.ev, next, data, effs)
		}
	}
}

func TestStopUnknown(t *testing.T) {
	if err := stop(context.Background(), proc.NewNode("")); err == nil {
		t.Error("stop of a machine not running succeeded")
	}
}
