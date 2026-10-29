package dist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// The messages of BenchmarkCodec, binary: the entries carry their
// commands with AppendAny.
type (
	binEntry struct {
		Term, Index uint64
		Cmd         any
	}
	binAppend struct {
		Term    uint64
		Leader  proc.PID
		Entries []binEntry
	}
	binReply struct {
		Term    uint64
		From    proc.PID
		Success bool
	}
	// binCmd is a command of a type gob alone encodes.
	binCmd struct{ N int }
)

func init() { Register(binAppend{}, binReply{}, binCmd{}) }

func (m binReply) AppendBinary(b []byte) ([]byte, error) {
	b = binary.AppendUvarint(b, m.Term)
	b, _ = AppendAny(b, m.From)
	return AppendAny(b, m.Success)
}

func (m *binReply) DecodeBinary(b []byte) error {
	var n int
	if m.Term, n = binary.Uvarint(b); n <= 0 {
		return errShort
	}
	from, b, err := ReadAny(b[n:])
	if err != nil {
		return err
	}
	ok, _, err := ReadAny(b)
	m.From, _ = from.(proc.PID)
	m.Success, _ = ok.(bool)
	return err
}

func (m binAppend) AppendBinary(b []byte) ([]byte, error) {
	b = binary.AppendUvarint(b, m.Term)
	b, _ = AppendAny(b, m.Leader)
	b = binary.AppendUvarint(b, uint64(len(m.Entries)))
	for _, e := range m.Entries {
		b = binary.AppendUvarint(binary.AppendUvarint(b, e.Term), e.Index)
		var err error
		if b, err = AppendAny(b, e.Cmd); err != nil {
			return b, err
		}
	}
	return b, nil
}

func (m *binAppend) DecodeBinary(b []byte) error {
	var n int
	if m.Term, n = binary.Uvarint(b); n <= 0 {
		return errShort
	}
	leader, b, err := ReadAny(b[n:])
	if err != nil {
		return err
	}
	m.Leader, _ = leader.(proc.PID)
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return errShort
	}
	b = b[n:]
	for range count {
		var e binEntry
		if e.Term, n = binary.Uvarint(b); n <= 0 {
			return errShort
		}
		b = b[n:]
		if e.Index, n = binary.Uvarint(b); n <= 0 {
			return errShort
		}
		if e.Cmd, b, err = ReadAny(b[n:]); err != nil {
			return err
		}
		m.Entries = append(m.Entries, e)
	}
	return nil
}

// AppendAny appends the values of the kinds it knows, of their own types,
// for ReadAny to read back as they were, one after the other.
func TestAppendAny(t *testing.T) {
	n := proc.NewNode("a@localhost")
	vals := []any{
		nil, true, false,
		int(-1), int8(-2), int16(-3), int32(-4), int64(-5),
		uint(1), uint8(2), uint16(3), uint32(4), uint64(1 << 63),
		float32(1.5), float64(-2.25),
		"", "text", []byte{}, []byte{1, 2, 3},
		n.NewPID(), n.MakeRef(),
		binReply{Term: 2, From: n.NewPID(), Success: true},
	}
	var b []byte
	for _, v := range vals {
		var err error
		if b, err = AppendAny(b, v); err != nil {
			t.Fatalf("%T: %v", v, err)
		}
	}
	for _, want := range vals {
		got, rest, err := ReadAny(b)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%#v, %v; want %#v", got, err, want)
		}
		b = rest
	}
	if len(b) != 0 {
		t.Errorf("%d bytes left", len(b))
	}
}

// A value of a type AppendAny does not know, a named one too, is not
// binary.
func TestAppendAnyNotBinary(t *testing.T) {
	type named int
	for _, v := range []any{binCmd{N: 1}, named(1), []int{1}, struct{}{}} {
		if _, err := AppendAny(nil, v); !errors.Is(err, ErrNotBinary) {
			t.Errorf("%T: %v, want ErrNotBinary", v, err)
		}
	}
}

// ReadAny fails on what is cut short, never reading past it.
func TestReadAnyCutShort(t *testing.T) {
	n := proc.NewNode("a@localhost")
	for _, v := range []any{int64(-1 << 40), uint64(1 << 40), 1.5, "text", []byte("bytes"), n.NewPID(), binReply{Term: 1, From: n.NewPID()}} {
		b, _ := AppendAny(nil, v)
		for i := range len(b) {
			if _, _, err := ReadAny(b[:i]); err == nil {
				t.Errorf("%T cut at %d of %d: no error", v, i, len(b))
			}
		}
	}
}

// A message of a binary type goes as its own bytes, out of the gob
// stream, the calls and casts of gen too; one whose AppendBinary is not
// binary for a command or request goes by gob. All come back as they
// went, between messages of gob.
func TestCodecBinary(t *testing.T) {
	n := proc.NewNode("a@localhost")
	msgs := []struct {
		msg    any
		binary bool
	}{
		{binReply{Term: 3, From: n.NewPID(), Success: true}, true},
		{"text", false},
		{binAppend{Term: 3, Leader: n.NewPID(), Entries: []binEntry{{Term: 3, Index: 1, Cmd: 1}, {Term: 3, Index: 2, Cmd: "x"}}}, true},
		{binAppend{Term: 3, Leader: n.NewPID(), Entries: []binEntry{{Term: 3, Index: 3, Cmd: binCmd{N: 1}}}}, false},
		{actor.CallMsg{Req: "call"}, true},
		{actor.CallMsg{From: actor.From{PID: n.NewPID(), Tag: n.MakeRef()}, Req: binReply{Term: 1}}, true},
		{actor.CallMsg{From: actor.From{PID: n.NewPID(), Tag: n.MakeRef()}, Req: binCmd{N: 2}}, false},
		{actor.CastMsg{Req: nil}, true},
		{actor.CastMsg{Req: binCmd{N: 3}}, false},
		{actor.CallAbandoned{From: actor.From{PID: n.NewPID(), Tag: n.MakeRef()}}, true},
		{binReply{Term: 4, From: n.NewPID()}, true},
	}
	enc, dec := gobCodec{}.NewEncoder(), gobCodec{}.NewDecoder()
	for i, m := range msgs {
		b, err := enc.Encode(m.msg)
		if err != nil {
			t.Fatalf("%d %T: %v", i, m.msg, err)
		}
		if got := b[0] == binaryMsg; got != m.binary {
			t.Errorf("%d %T: binary %v, want %v", i, m.msg, got, m.binary)
		}
		got, err := dec.Decode(b)
		if err != nil || !reflect.DeepEqual(got, m.msg) {
			t.Errorf("%d: %#v, %v; want %#v", i, got, err, m.msg)
		}
	}
}

// BenchmarkCodecBinary is BenchmarkCodec, with the messages binary.
func BenchmarkCodecBinary(b *testing.B) {
	n := proc.NewNode("a@localhost")
	msgs := []any{
		binAppend{Term: 3, Leader: n.NewPID(), Entries: []binEntry{{Term: 3, Index: 42, Cmd: 1}}},
		binReply{Term: 3, From: n.NewPID(), Success: true},
	}
	enc, dec := gobCodec{}.NewEncoder(), gobCodec{}.NewDecoder()
	b.ReportAllocs()
	for b.Loop() {
		for _, m := range msgs {
			p, err := enc.Encode(m)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := dec.Decode(p); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func TestReason(t *testing.T) {
	wrapped := fmt.Errorf("stopping: %w", proc.Shutdown)
	for _, err := range []error{nil, proc.Normal, proc.Killed, proc.NoConnection, errors.New("boom"), wrapped} {
		b := AppendReason([]byte{7}, err)
		got, rest, rerr := ReadReason(b[1:])
		if rerr != nil || len(rest) != 0 {
			t.Fatalf("%v: %v, %d left", err, rerr, len(rest))
		}
		want := err
		if want == nil {
			want = proc.Normal
		}
		if got.Error() != want.Error() || errors.Is(want, proc.Shutdown) != errors.Is(got, proc.Shutdown) {
			t.Fatalf("%v read as %v", err, got)
		}
	}
	if _, _, err := ReadReason(nil); err == nil {
		t.Fatal("read a reason of nothing")
	}
}
