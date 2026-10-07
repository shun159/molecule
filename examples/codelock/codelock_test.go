package main

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genstatem"
	"github.com/shun159/molecule/proc"
)

func TestScenario(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var out bytes.Buffer
		start := time.Now()
		if err := run(&out, 2*time.Second); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"door locked",
			"wrong code",
			"correct code",
			"door open",
			"status: open",
			"door locked",
			"wrong code", // the buttons postponed while open
			"status: locked",
			"code cleared",
			"correct code",
			"door open",
		}
		if got := strings.Split(strings.TrimSpace(out.String()), "\n"); !reflect.DeepEqual(got, want) {
			t.Errorf("display:\n%s\nwant:\n%s", out.String(), strings.Join(want, "\n"))
		}
		if d := time.Since(start); d != 3*time.Second+clearTime+time.Second {
			t.Errorf("took %v", d)
		}
	})
}

// The lock is pure: its transitions are tested as plain function calls.
func TestCodeLockPure(t *testing.T) {
	display := proc.NewNode("").Spawn(func(*proc.Self) error { return nil }) // any PID will do
	l := CodeLock{Code: []int{1, 2}, OpenTime: time.Second, Display: display}
	press := func(n int) genstatem.Event { return genstatem.Cast{Msg: Button{n}} }

	state, data, effs := l.HandleEvent(Locked, Data{}, press(1))
	if state != Locked || !reflect.DeepEqual(data.Buttons, []int{1}) ||
		!reflect.DeepEqual(effs, gen.Do(genstatem.StartEventTimeout{After: clearTime, Msg: clear{}})) {
		t.Errorf("first button: %v %+v %#v", state, data, effs)
	}
	state, _, effs = l.HandleEvent(state, data, press(2))
	if state != Open || !reflect.DeepEqual(effs, gen.Do(gen.Send{To: display, Msg: "correct code"})) {
		t.Errorf("right code: %v %#v", state, effs)
	}
	_, _, effs = l.HandleEvent(Open, Data{}, genstatem.Enter[State]{Old: Locked})
	want := gen.Do(gen.Send{To: display, Msg: "door open"}, genstatem.StartStateTimeout{After: time.Second, Msg: lock{}})
	if !reflect.DeepEqual(effs, want) {
		t.Errorf("entering Open: %#v", effs)
	}
	if _, _, effs = l.HandleEvent(Open, Data{}, press(1)); !reflect.DeepEqual(effs, gen.Do(genstatem.Postpone{})) {
		t.Errorf("button while open: %#v", effs)
	}
	if state, _, _ = l.HandleEvent(Open, Data{}, genstatem.StateTimeout{Msg: lock{}}); state != Locked {
		t.Errorf("state timeout in Open: %v", state)
	}
}
