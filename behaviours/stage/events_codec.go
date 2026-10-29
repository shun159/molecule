package stage

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/shun159/molecule/dist"
)

// The events of a message go between nodes as dist's binary types: a
// count, then each event, for the integers, floats, strings, byte slices
// and bools themselves, for a type T encoding itself, as
// encoding.BinaryAppender, *T dist.BinaryDecoder, each after its length.
// Of another type, they go by gob.

func (e events[T]) AppendBinary(b []byte) ([]byte, error) {
	b = binary.AppendUvarint(b, uint64(len(e)))
	if b, ok := appendBasic(b, []T(e)); ok {
		return b, nil
	}
	if !encodesItself[T]() {
		var zero T
		return b, fmt.Errorf("%w: events of %T", dist.ErrNotBinary, zero)
	}
	for i := range e {
		var err error
		if b, err = appendSized(b, any(&e[i]).(encoding.BinaryAppender)); err != nil {
			return b, err
		}
	}
	return b, nil
}

func (e *events[T]) DecodeBinary(b []byte) error {
	n, size := binary.Uvarint(b)
	if size <= 0 || n > uint64(len(b)) { // an event is a byte at least
		return errShort
	}
	b = b[size:]
	out := make([]T, n)
	if ok, err := readBasic(b, out); ok {
		*e = out
		return err
	}
	for i := range out {
		d, ok := any(&out[i]).(dist.BinaryDecoder)
		if !ok {
			return fmt.Errorf("%w: events of %T", dist.ErrNotBinary, out[i])
		}
		var err error
		if b, err = readSized(b, d); err != nil {
			return err
		}
	}
	*e = out
	return nil
}

// encodesItself reports whether T encodes itself: T as an
// encoding.BinaryAppender, *T as a dist.BinaryDecoder.
func encodesItself[T any]() bool {
	var zero T
	_, appends := any(&zero).(encoding.BinaryAppender)
	_, decodes := any(&zero).(dist.BinaryDecoder)
	return appends && decodes
}

// appendSized appends what v appends, after its length.
func appendSized(b []byte, v encoding.BinaryAppender) ([]byte, error) {
	at := len(b)
	b, err := v.AppendBinary(append(b, 0, 0, 0, 0))
	binary.BigEndian.PutUint32(b[at:], uint32(len(b)-at-4))
	return b, err
}

// readSized reads into d what appendSized appended, and returns the rest.
func readSized(b []byte, d dist.BinaryDecoder) ([]byte, error) {
	if len(b) < 4 {
		return b, errShort
	}
	n := int(binary.BigEndian.Uint32(b))
	if len(b) < 4+n {
		return b, errShort
	}
	return b[4+n:], d.DecodeBinary(b[4 : 4+n])
}

// appendBasic appends the events of the types encoded as themselves,
// and reports whether they are of one: a table, a line a type.
func appendBasic[T any](b []byte, events []T) ([]byte, bool) {
	switch s := any(events).(type) {
	case []int:
		return appendInts(b, s), true
	case []int64:
		return appendInts(b, s), true
	case []int32:
		return appendInts(b, s), true
	case []uint64:
		return appendUints(b, s), true
	case []uint32:
		return appendUints(b, s), true
	case []float64:
		return appendFloats(b, s), true
	case []string:
		return appendBytes(b, s), true
	case [][]byte:
		return appendBytes(b, s), true
	case []bool:
		return appendBools(b, s), true
	}
	return b, false
}

// readBasic reads the events of the types encoded as themselves into
// out, and reports whether they are of one: a table, a line a type.
func readBasic[T any](b []byte, out []T) (bool, error) {
	switch s := any(out).(type) {
	case []int:
		return true, readInts(b, s)
	case []int64:
		return true, readInts(b, s)
	case []int32:
		return true, readInts(b, s)
	case []uint64:
		return true, readUints(b, s)
	case []uint32:
		return true, readUints(b, s)
	case []float64:
		return true, readFloats(b, s)
	case []string:
		return true, readBytes(b, s, func(x []byte) string { return string(x) })
	case [][]byte:
		return true, readBytes(b, s, bytes.Clone)
	case []bool:
		return true, readBools(b, s)
	}
	return false, nil
}

func appendInts[I int | int64 | int32](b []byte, s []I) []byte {
	for _, x := range s {
		b = binary.AppendVarint(b, int64(x))
	}
	return b
}

func readInts[I int | int64 | int32](b []byte, s []I) error {
	for i := range s {
		x, n := binary.Varint(b)
		if n <= 0 {
			return errShort
		}
		s[i], b = I(x), b[n:]
	}
	return nil
}

func appendUints[U uint64 | uint32](b []byte, s []U) []byte {
	for _, x := range s {
		b = binary.AppendUvarint(b, uint64(x))
	}
	return b
}

func readUints[U uint64 | uint32](b []byte, s []U) error {
	for i := range s {
		x, n := binary.Uvarint(b)
		if n <= 0 {
			return errShort
		}
		s[i], b = U(x), b[n:]
	}
	return nil
}

func appendFloats(b []byte, s []float64) []byte {
	for _, x := range s {
		b = binary.BigEndian.AppendUint64(b, math.Float64bits(x))
	}
	return b
}

func readFloats(b []byte, s []float64) error {
	if len(b) < 8*len(s) {
		return errShort
	}
	for i := range s {
		s[i] = math.Float64frombits(binary.BigEndian.Uint64(b[8*i:]))
	}
	return nil
}

// appendBytes appends each byte string after its length.
func appendBytes[E string | []byte](b []byte, s []E) []byte {
	for _, x := range s {
		b = append(binary.AppendUvarint(b, uint64(len(x))), x...)
	}
	return b
}

// readBytes reads the length-prefixed byte strings into s, each copied
// by as: what is decoded keeps nothing of b.
func readBytes[E any](b []byte, s []E, as func([]byte) E) error {
	for i := range s {
		l, n := binary.Uvarint(b)
		if n <= 0 || uint64(len(b)-n) < l {
			return errShort
		}
		s[i], b = as(b[n:n+int(l)]), b[n+int(l):]
	}
	return nil
}

func appendBools(b []byte, s []bool) []byte {
	for _, x := range s {
		c := byte(0)
		if x {
			c = 1
		}
		b = append(b, c)
	}
	return b
}

func readBools(b []byte, s []bool) error {
	if len(b) < len(s) {
		return errShort
	}
	for i := range s {
		s[i] = b[i] != 0
	}
	return nil
}
