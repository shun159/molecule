package stage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// Stages of different nodes exchange their messages through dist. The
// messages of the protocol encode themselves, as dist's binary types: they
// are small and many, and carry reasons, which gob does not encode. So do
// the events, of the types registered with Register, see events_codec.go,
// or else go by gob.

// Register tells the distribution the type T of events, for stages of
// different nodes to exchange them, as dist.Register does messages: each
// node registers the types of the events it sends or receives, at init.
// Events of the integers, floats, strings, byte slices and bools are sent
// as themselves; those of a type T with AppendBinary, as
// encoding.BinaryAppender, and *T with DecodeBinary, as
// dist.BinaryDecoder, as these encode them; others by gob, with their
// exported fields.
func Register[T any]() { dist.Register(events[T](nil)) }

func init() {
	dist.Register(subscribeMsg{}, askMsg{}, eventsMsg{}, cancelMsg{}, cancelledMsg{})
}

var errShort = errors.New("stage: message cut short")

func (m subscribeMsg) AppendBinary(b []byte) ([]byte, error) {
	b, err := m.Ref.appendTo(b)
	b = binary.AppendVarint(b, int64(m.Ask))
	return binary.AppendVarint(b, int64(m.Partition)), err
}

func (m *subscribeMsg) DecodeBinary(b []byte) error {
	ref, b, err := readSubRef(b)
	if err != nil {
		return err
	}
	ask, b, err := readInt(b)
	if err != nil {
		return err
	}
	partition, _, err := readInt(b)
	*m = subscribeMsg{Ref: ref, Ask: ask, Partition: partition}
	return err
}

func (m eventsMsg) AppendBinary(b []byte) ([]byte, error) {
	b, err := m.Ref.appendTo(b)
	if err != nil {
		return b, err
	}
	return dist.AppendAny(b, m.Events)
}

func (m *eventsMsg) DecodeBinary(b []byte) error {
	ref, b, err := readSubRef(b)
	if err != nil {
		return err
	}
	v, _, err := dist.ReadAny(b)
	if err != nil {
		return err
	}
	events, ok := v.(batch)
	if !ok {
		return fmt.Errorf("stage: events of %T", v)
	}
	*m = eventsMsg{Ref: ref, Events: events}
	return nil
}

func (m askMsg) AppendBinary(b []byte) ([]byte, error) {
	b, err := m.Ref.appendTo(b)
	return binary.AppendVarint(b, int64(m.N)), err
}

func (m *askMsg) DecodeBinary(b []byte) error {
	ref, b, err := readSubRef(b)
	if err != nil {
		return err
	}
	n, _, err := readInt(b)
	*m = askMsg{Ref: ref, N: n}
	return err
}

func (m cancelMsg) AppendBinary(b []byte) ([]byte, error) {
	b, err := m.Ref.appendTo(b)
	return appendReason(b, m.Reason), err
}

func (m *cancelMsg) DecodeBinary(b []byte) error {
	ref, reason, err := readCancel(b)
	*m = cancelMsg{Ref: ref, Reason: reason}
	return err
}

func (m cancelledMsg) AppendBinary(b []byte) ([]byte, error) {
	b, err := m.Ref.appendTo(b)
	return appendReason(b, m.Reason), err
}

func (m *cancelledMsg) DecodeBinary(b []byte) error {
	ref, reason, err := readCancel(b)
	*m = cancelledMsg{Ref: ref, Reason: reason}
	return err
}

// readCancel reads the subRef and the reason of a cancel, either way.
func readCancel(b []byte) (subRef, error, error) {
	ref, b, err := readSubRef(b)
	if err != nil {
		return ref, nil, err
	}
	reason, _, err := readReason(b)
	return ref, reason, err
}

func (r subRef) appendTo(b []byte) ([]byte, error) {
	b, err := dist.AppendAny(b, r.Consumer)
	return binary.AppendUvarint(b, r.N), err
}

func readSubRef(b []byte) (subRef, []byte, error) {
	v, b, err := dist.ReadAny(b)
	if err != nil {
		return subRef{}, b, err
	}
	pid, ok := v.(proc.PID)
	n, size := binary.Uvarint(b)
	if !ok || size <= 0 {
		return subRef{}, b, errShort
	}
	return subRef{Consumer: pid, N: n}, b[size:], nil
}

func readInt(b []byte) (int, []byte, error) {
	n, size := binary.Varint(b)
	if size <= 0 {
		return 0, b, errShort
	}
	return int(n), b[size:], nil
}

// The errors of stage, sent by their code, for errors.Is to tell them on
// the other node; 0 is another, sent as dist sends reasons.
var reasons = []error{nil, ErrBadDemand, ErrBadPartition, ErrPartitionTaken, ErrNoPartitions, ErrBufferFull}

func appendReason(b []byte, err error) []byte {
	if i := slices.Index(reasons[1:], err); i >= 0 {
		return append(b, byte(i+1))
	}
	return dist.AppendReason(append(b, 0), err)
}

func readReason(b []byte) (error, []byte, error) {
	if len(b) == 0 {
		return nil, b, errShort
	}
	code, b := int(b[0]), b[1:]
	switch {
	case code >= len(reasons):
		return nil, b, errShort
	case code > 0:
		return reasons[code], b, nil
	}
	return dist.ReadReason(b)
}
