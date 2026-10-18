package dist

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"

	"github.com/shun159/molecule/proc"
)

// A registered type whose values have AppendBinary, as
// encoding.BinaryAppender, and whose pointers DecodeBinary, as
// BinaryDecoder, is sent as the bytes AppendBinary appends, after the ID
// of its type, not by gob: gob describes and reads a value by reflection,
// several times the cost of a small message.
//
// A value inside such a message of an interface type, such as a command
// of the application, is appended with AppendAny and read with ReadAny.
// Of a type they cannot encode, AppendAny returns ErrNotBinary: returned
// by AppendBinary, wrapped or not, it has the message sent by gob, as of
// a type without the methods.

// BinaryDecoder decodes a value from what its AppendBinary appended,
// copying what it keeps of b. It is not encoding.BinaryUnmarshaler, which
// gob would use to decode a message sent by gob, where AppendAny failed.
type BinaryDecoder interface {
	DecodeBinary(b []byte) error
}

// ErrNotBinary tells that a value has no binary encoding: see AppendAny.
var ErrNotBinary = errors.New("dist: no binary encoding")

// The kinds of values AppendAny appends, in the byte before each.
const (
	anyNil byte = iota
	anyFalse
	anyTrue
	anyInt
	anyInt8
	anyInt16
	anyInt32
	anyInt64
	anyUint
	anyUint8
	anyUint16
	anyUint32
	anyUint64
	anyFloat32
	anyFloat64
	anyString
	anyBytes
	anyPID
	anyRef
	anyTyped // a registered binary type, by the ID of its type
)

// AppendAny appends v to b, for ReadAny to read: nil, a bool, an integer,
// a float, a string, a []byte, a PID, a Ref, or a value of a registered
// binary type. Of another type, it returns ErrNotBinary.
//
// It is kept a table, a line a kind: split, every value paid a call more.
//
//abc:disable abc a table of the kinds of value, on the path of every value sent
//abc:hot BenchmarkCodecBinary
func AppendAny(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, anyNil), nil
	case bool:
		if x {
			return append(b, anyTrue), nil
		}
		return append(b, anyFalse), nil
	case int:
		return binary.AppendVarint(append(b, anyInt), int64(x)), nil
	case int8:
		return binary.AppendVarint(append(b, anyInt8), int64(x)), nil
	case int16:
		return binary.AppendVarint(append(b, anyInt16), int64(x)), nil
	case int32:
		return binary.AppendVarint(append(b, anyInt32), int64(x)), nil
	case int64:
		return binary.AppendVarint(append(b, anyInt64), x), nil
	case uint:
		return binary.AppendUvarint(append(b, anyUint), uint64(x)), nil
	case uint8:
		return binary.AppendUvarint(append(b, anyUint8), uint64(x)), nil
	case uint16:
		return binary.AppendUvarint(append(b, anyUint16), uint64(x)), nil
	case uint32:
		return binary.AppendUvarint(append(b, anyUint32), uint64(x)), nil
	case uint64:
		return binary.AppendUvarint(append(b, anyUint64), x), nil
	case float32:
		return binary.BigEndian.AppendUint32(append(b, anyFloat32), math.Float32bits(x)), nil
	case float64:
		return binary.BigEndian.AppendUint64(append(b, anyFloat64), math.Float64bits(x)), nil
	case string:
		return append(binary.AppendUvarint(append(b, anyString), uint64(len(x))), x...), nil
	case []byte:
		return append(binary.AppendUvarint(append(b, anyBytes), uint64(len(x))), x...), nil
	case proc.PID:
		return appendSized(append(b, anyPID), x.AppendBinary)
	case proc.Ref:
		return appendSized(append(b, anyRef), x.AppendBinary)
	}
	t, ok := typeOf(v)
	if !ok || t.bin == nil {
		return b, fmt.Errorf("%w: %T", ErrNotBinary, v)
	}
	b = binary.BigEndian.AppendUint64(append(b, anyTyped), t.id)
	return appendSized(b, func(b []byte) ([]byte, error) { return t.bin.append(b, v) })
}

// appendSized appends what appendTo appends, after its length in 4 bytes.
func appendSized(b []byte, appendTo func([]byte) ([]byte, error)) ([]byte, error) {
	at := len(b)
	b, err := appendTo(append(b, 0, 0, 0, 0))
	if err != nil {
		return b, err
	}
	binary.BigEndian.PutUint32(b[at:], uint32(len(b)-at-4))
	return b, nil
}

var errShort = errors.New("dist: binary value cut short")

// ReadAny reads the value AppendAny appended at the start of b, and
// returns it with the rest of b.
//
//abc:hot BenchmarkCodecBinary
func ReadAny(b []byte) (any, []byte, error) {
	if len(b) == 0 {
		return nil, b, errShort
	}
	kind, b := b[0], b[1:]
	switch kind {
	case anyNil:
		return nil, b, nil
	case anyFalse, anyTrue:
		return kind == anyTrue, b, nil
	case anyInt, anyInt8, anyInt16, anyInt32, anyInt64:
		x, n := binary.Varint(b)
		if n <= 0 {
			return nil, b, errShort
		}
		return signed(kind, x), b[n:], nil
	case anyUint, anyUint8, anyUint16, anyUint32, anyUint64:
		x, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, b, errShort
		}
		return unsigned(kind, x), b[n:], nil
	case anyFloat32, anyFloat64:
		return readFloat(kind, b)
	case anyString, anyBytes:
		return readLengthed(kind, b)
	case anyPID, anyRef, anyTyped:
		return readSized(kind, b)
	}
	return nil, b, fmt.Errorf("dist: binary value of an unknown kind %d", kind)
}

func signed(kind byte, x int64) any {
	switch kind {
	case anyInt8:
		return int8(x)
	case anyInt16:
		return int16(x)
	case anyInt32:
		return int32(x)
	case anyInt64:
		return x
	}
	return int(x)
}

func unsigned(kind byte, x uint64) any {
	switch kind {
	case anyUint8:
		return uint8(x)
	case anyUint16:
		return uint16(x)
	case anyUint32:
		return uint32(x)
	case anyUint64:
		return x
	}
	return uint(x)
}

// readFloat reads a float of the kind, as its bits in 4 or 8 bytes.
func readFloat(kind byte, b []byte) (any, []byte, error) {
	if kind == anyFloat32 {
		if len(b) < 4 {
			return nil, b, errShort
		}
		return math.Float32frombits(binary.BigEndian.Uint32(b)), b[4:], nil
	}
	if len(b) < 8 {
		return nil, b, errShort
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), b[8:], nil
}

// readLengthed reads a string or a []byte, of the kind, after its length
// as a uvarint.
func readLengthed(kind byte, b []byte) (any, []byte, error) {
	l, n := binary.Uvarint(b)
	if n <= 0 || uint64(len(b)-n) < l {
		return nil, b, errShort
	}
	s, rest := b[n:n+int(l)], b[n+int(l):]
	if kind == anyString {
		return string(s), rest, nil
	}
	return slices.Clone(s), rest, nil
}

// readSized reads a PID, a Ref or a value of a binary type, after its
// length.
func readSized(kind byte, b []byte) (any, []byte, error) {
	var t typeID
	if kind == anyTyped {
		var err error
		if t, b, err = readTypeID(b); err != nil {
			return nil, b, err
		}
	}
	if len(b) < 4 || uint64(len(b)-4) < uint64(binary.BigEndian.Uint32(b)) {
		return nil, b, errShort
	}
	l := int(binary.BigEndian.Uint32(b))
	v, rest := b[4:4+l], b[4+l:]
	switch kind {
	case anyPID:
		var p proc.PID
		err := p.UnmarshalBinary(v)
		return p, rest, err
	case anyRef:
		var r proc.Ref
		err := r.UnmarshalBinary(v)
		return r, rest, err
	}
	x, err := t.bin.decode(v)
	return x, rest, err
}

// readTypeID reads the ID of a registered binary type.
func readTypeID(b []byte) (typeID, []byte, error) {
	if len(b) < 8 {
		return typeID{}, b, errShort
	}
	id := binary.BigEndian.Uint64(b)
	t, ok := typeByID(id)
	if !ok || t.bin == nil {
		return typeID{}, b, fmt.Errorf("dist: a binary value of a type not registered, %#x", id)
	}
	return t, b[8:], nil
}

// binaryCodec encodes the values of a binary type.
type binaryCodec struct {
	append func(b []byte, v any) ([]byte, error)
	decode func(b []byte) (any, error)
}

// methodsOf returns the codec of the methods of t, nil if it has none.
func methodsOf(t reflect.Type) *binaryCodec {
	_, app := reflect.Zero(t).Interface().(encoding.BinaryAppender)
	_, dec := reflect.New(t).Interface().(BinaryDecoder)
	if !app || !dec {
		return nil
	}
	return &binaryCodec{
		append: func(b []byte, v any) ([]byte, error) {
			return v.(encoding.BinaryAppender).AppendBinary(b)
		},
		decode: func(b []byte) (any, error) {
			v := reflect.New(t)
			if err := v.Interface().(BinaryDecoder).DecodeBinary(b); err != nil {
				return nil, err
			}
			return v.Elem().Interface(), nil
		},
	}
}
