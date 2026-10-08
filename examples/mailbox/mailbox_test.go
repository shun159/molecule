package main

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

func TestRun(t *testing.T) {
	for _, args := range [][]string{{"3", "1000"}, {"10", "1"}, {"0", "5"}, {"4", "0"}} {
		var out bytes.Buffer
		if err := run(args, &out); err != nil {
			t.Fatalf("run(%v): %v", args, err)
		}
		size, pass, _ := parseArgs(args)
		want := fmt.Sprintf("received %d pongs from %d mailers × %d messages\n", size*pass, size, pass)
		if !strings.HasPrefix(out.String(), want) {
			t.Errorf("run(%v) printed %q, want it to start with %q", args, out.String(), want)
		}
	}
}

func TestUsage(t *testing.T) {
	for _, args := range [][]string{nil, {"3"}, {"x", "1"}, {"1", "-2"}, {"1", "2", "3"}} {
		if err := run(args, &bytes.Buffer{}); !errors.Is(err, errUsage) {
			t.Errorf("run(%v) = %v, want the usage", args, err)
		}
	}
}

// The receiver is pure: the counting is tested without processes.
func TestReceiverPure(t *testing.T) {
	r := Receiver{Expect: 2}
	var from genserver.From[int]
	s, _, _ := r.Init(proc.PID{})

	s, effs := r.HandleCall(s, Wait{}, from)
	if effs != nil || len(s.waiting) != 1 {
		t.Fatalf("Wait before all are in: %+v, %#v", s, effs)
	}
	s, effs = r.HandleCast(s, Pong{})
	if effs != nil {
		t.Fatalf("answered early: %#v", effs)
	}
	s, effs = r.HandleCast(s, Pong{})
	if !reflect.DeepEqual(effs, molecule.Do(from.Reply(2))) || len(s.waiting) != 0 {
		t.Errorf("on the last pong: %+v, %#v", s, effs)
	}
	if _, effs = r.HandleCall(s, Wait{}, from); !reflect.DeepEqual(effs, molecule.Do(from.Reply(2))) {
		t.Errorf("Wait after all are in: %#v", effs)
	}
}
