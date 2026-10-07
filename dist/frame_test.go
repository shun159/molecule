package dist

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/shun159/molecule/proc"
)

func TestFrameRoundTrip(t *testing.T) {
	n := proc.NewNode("a@test")
	pid, other := n.NewPID(), n.NewPID()
	ref := n.MakeRef()
	for _, f := range []frame{
		{op: opSend, to: pid, payload: []byte("msg")},
		{op: opSendName, name: "server", payload: []byte("msg")},
		{op: opSendAlias, ref: ref, payload: []byte{}},
		{op: opExit, from: pid, to: other, reason: proc.Kill},
		{op: opExitLink, from: pid, to: other, reason: proc.Normal},
		{op: opLink, from: pid, to: other},
		{op: opUnlink, from: pid, to: other},
		{op: opMonitor, ref: ref, to: pid},
		{op: opDemonitor, ref: ref, to: pid},
		{op: opDown, ref: ref, to: pid, reason: proc.NoProc},
		{op: opMonitorName, ref: ref, name: "server"},
		{op: opDemonitorName, ref: ref, name: "server"},
	} {
		got, err := parseFrame(appendFrame(nil, f))
		if f.payload == nil {
			f.payload = []byte{}
		}
		if err != nil || !reflect.DeepEqual(got, f) {
			t.Errorf("op %d: got %+v, %v; want %+v", f.op, got, err, f)
		}
	}
	for _, b := range [][]byte{nil, {0}, {byte(opSend), 0}, {byte(opExit), 0, 1}} {
		if _, err := parseFrame(b); err == nil {
			t.Errorf("parsed %v", b)
		}
	}
}

func TestReasons(t *testing.T) {
	boom := errors.New("boom")
	for _, tt := range []struct {
		in    error
		check func(error) bool
	}{
		{nil, func(e error) bool { return e == proc.Normal }},
		{proc.Normal, func(e error) bool { return e == proc.Normal }},
		{proc.Shutdown, func(e error) bool { return e == proc.Shutdown }},
		{proc.Kill, func(e error) bool { return e == proc.Kill }},
		{proc.Killed, func(e error) bool { return e == proc.Killed }},
		{proc.NoProc, func(e error) bool { return e == proc.NoProc }},
		{proc.NoConnection, func(e error) bool { return e == proc.NoConnection }},
		{boom, func(e error) bool {
			var re *RemoteError
			return errors.As(e, &re) && re.Text == "boom" && re.Wraps == nil
		}},
		{fmt.Errorf("%w: stopping", proc.Shutdown), func(e error) bool {
			return errors.Is(e, proc.Shutdown) && e.Error() == "shutdown: stopping" && !errors.Is(e, proc.Normal)
		}},
		{fmt.Errorf("%w: done", proc.Normal), func(e error) bool {
			return errors.Is(e, proc.Normal) && !proc.IsAbnormal(e)
		}},
	} {
		r := reader{b: appendReason(nil, tt.in)}
		got := r.reason()
		if r.err != nil || len(r.b) != 0 || !tt.check(got) {
			t.Errorf("reason %v came as %#v, %v", tt.in, got, r.err)
		}
	}
}

// TestGobNewStream fails an encoding: the encoder starts a new stream,
// which the decoder follows.
func TestGobNewStream(t *testing.T) {
	enc, dec := Gob.NewEncoder(), Gob.NewDecoder()
	send := func(v any) (any, error) {
		b, err := enc.Encode(v)
		if err != nil {
			return nil, err
		}
		return dec.Decode(b)
	}
	type local struct{ X int } // not registered
	for _, v := range []any{1, "two"} {
		if got, err := send(v); err != nil || got != v {
			t.Errorf("%v came as %v, %v", v, got, err)
		}
	}
	if _, err := send(local{3}); err == nil {
		t.Error("unregistered type encoded")
	}
	for _, v := range []any{4, "five", 4} {
		if got, err := send(v); err != nil || got != v {
			t.Errorf("after the failure, %v came as %v, %v", v, got, err)
		}
	}
	pid := proc.NewNode("a@test").NewPID()
	if got, err := send(pid); err != nil || got != pid {
		t.Errorf("PID came as %v, %v", got, err)
	}
}
