package stage

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// point encodes itself.
type point struct{ X, Y int32 }

func (p point) AppendBinary(b []byte) ([]byte, error) {
	return binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(b, uint32(p.X)), uint32(p.Y)), nil
}

func (p *point) DecodeBinary(b []byte) error {
	if len(b) != 8 {
		return errShort
	}
	p.X, p.Y = int32(binary.BigEndian.Uint32(b)), int32(binary.BigEndian.Uint32(b[4:]))
	return nil
}

// plain does not encode itself.
type plain struct{ N int }

func roundTrip[T any](t *testing.T, e events[T]) {
	t.Helper()
	b, err := e.AppendBinary([]byte{0xff})
	if err != nil {
		t.Fatalf("%T: %v", e, err)
	}
	var got events[T]
	if err := got.DecodeBinary(b[1:]); err != nil {
		t.Fatalf("%T: %v", e, err)
	}
	if !reflect.DeepEqual(got, e) {
		t.Fatalf("%v read as %v", e, got)
	}
	if len(e) > 0 {
		if err := got.DecodeBinary(b[1 : len(b)-1]); err == nil {
			t.Fatalf("%T read cut short", e)
		}
	}
}

func TestEventsCodec(t *testing.T) {
	roundTrip(t, events[int]{0, -1, 1 << 40, -(1 << 62)})
	roundTrip(t, events[int32]{-7, 7})
	roundTrip(t, events[int64]{1 << 50})
	roundTrip(t, events[uint64]{0, 1 << 63})
	roundTrip(t, events[uint32]{1 << 31})
	roundTrip(t, events[float64]{0, -1.5, 3e300})
	roundTrip(t, events[string]{"", "molecule", "ストリーム"})
	roundTrip(t, events[[]byte]{{}, {1, 2, 3}})
	roundTrip(t, events[bool]{true, false})
	roundTrip(t, events[point]{{1, -2}, {3, 4}})
	roundTrip(t, events[int]{})
}

// TestEventsCodecNotBinary has events of a type not encoding itself go
// by gob.
func TestEventsCodecNotBinary(t *testing.T) {
	_, err := events[plain]{{1}}.AppendBinary(nil)
	if !errors.Is(err, dist.ErrNotBinary) {
		t.Fatalf("plain events: %v", err)
	}
	msg := eventsMsg{Ref: subRef{Consumer: proc.NewNode("a@codec").NewPID()}, Events: events[plain]{{1}}}
	if _, err := msg.AppendBinary(nil); !errors.Is(err, dist.ErrNotBinary) {
		t.Fatalf("a message of plain events: %v", err)
	}
}

// TestEventsCodecHuge has a count of events larger than the bytes after
// it refused, before any is made.
func TestEventsCodecHuge(t *testing.T) {
	var e events[int]
	if err := e.DecodeBinary(binary.AppendUvarint(nil, 1<<60)); err == nil {
		t.Fatal("read 2^60 events of nothing")
	}
}
