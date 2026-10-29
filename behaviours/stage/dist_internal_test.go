package stage

import (
	"errors"
	"testing"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// binaryMsg is a message of the protocol, encoding itself.
type binaryMsg interface {
	AppendBinary(b []byte) ([]byte, error)
}

// TestWire has the messages of the protocol read as written, their
// reasons as errors.Is tells them.
func TestWire(t *testing.T) {
	n := proc.NewNode("a@wire")
	ref := subRef{Consumer: n.NewPID(), N: 1 << 40}
	cases := []struct {
		msg  binaryMsg
		read func([]byte) (any, error)
	}{
		{subscribeMsg{Ref: ref, Ask: 1000, Partition: 3}, decoded[subscribeMsg]},
		{askMsg{Ref: ref, N: -1}, decoded[askMsg]},
		{cancelMsg{Ref: ref}, decoded[cancelMsg]},
		{cancelMsg{Ref: ref, Reason: errors.New("boom")}, decoded[cancelMsg]},
		{cancelledMsg{Ref: ref, Reason: ErrPartitionTaken}, decoded[cancelledMsg]},
		{cancelledMsg{Ref: ref, Reason: proc.Shutdown}, decoded[cancelledMsg]},
	}
	for _, c := range cases {
		b, err := c.msg.AppendBinary(nil)
		if err != nil {
			t.Fatalf("%+v: %v", c.msg, err)
		}
		got, err := c.read(b)
		if err != nil {
			t.Fatalf("%+v: %v", c.msg, err)
		}
		if !sameMsg(got, c.msg) {
			t.Fatalf("%+v read as %+v", c.msg, got)
		}
		if _, err := c.read(b[:len(b)-1]); err == nil {
			t.Fatalf("%+v read cut short", c.msg)
		}
	}
}

func decoded[M any, P interface {
	*M
	DecodeBinary([]byte) error
}](b []byte) (any, error) {
	var m M
	err := P(&m).DecodeBinary(b)
	return m, err
}

// sameMsg compares messages, their reasons by text and by what errors.Is
// tells of them: the text of an error of no code is all of it that
// crosses.
func sameMsg(got, want any) bool {
	reasonOf := func(m any) (any, bool, error) {
		switch x := m.(type) {
		case cancelMsg:
			r := x.Reason
			x.Reason = nil
			return x, true, r
		case cancelledMsg:
			r := x.Reason
			x.Reason = nil
			return x, true, r
		}
		return m, false, nil
	}
	g, _, gr := reasonOf(got)
	w, cancels, wr := reasonOf(want)
	if g != w {
		return false
	}
	if !cancels {
		return true
	}
	if wr == nil {
		return errors.Is(gr, proc.Normal)
	}
	var remote *dist.RemoteError
	if errors.As(gr, &remote) {
		return remote.Text == wr.Error()
	}
	return gr == wr
}
