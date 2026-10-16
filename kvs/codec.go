package kvs

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/gob"
	"errors"
)

// Codec turns the keys or the values of a table into bytes and back, for
// the log.
type Codec[T any] interface {
	// Append appends the encoding of v to b.
	Append(b []byte, v T) ([]byte, error)
	// Decode decodes what Append encoded. b is not kept by the table
	// after: a value keeping bytes of it copies them.
	Decode(b []byte) (T, error)
}

// DefaultCodec returns the codec of a table for T when its Options give
// none: a plain one for strings, byte slices and integers; MarshalBinary
// and UnmarshalBinary for a type with them, such as netip.Addr; gob for
// the others.
func DefaultCodec[T any]() Codec[T] {
	var zero T
	var c any
	switch any(zero).(type) {
	case string:
		c = stringCodec{}
	case []byte:
		c = bytesCodec{}
	case int:
		c = intCodec[int]{}
	case int8:
		c = intCodec[int8]{}
	case int16:
		c = intCodec[int16]{}
	case int32:
		c = intCodec[int32]{}
	case int64:
		c = intCodec[int64]{}
	case uint:
		c = uintCodec[uint]{}
	case uint8:
		c = uintCodec[uint8]{}
	case uint16:
		c = uintCodec[uint16]{}
	case uint32:
		c = uintCodec[uint32]{}
	case uint64:
		c = uintCodec[uint64]{}
	}
	if c != nil {
		return c.(Codec[T])
	}
	_, m := any(zero).(encoding.BinaryMarshaler)
	_, u := any(&zero).(encoding.BinaryUnmarshaler)
	if m && u {
		return binaryCodec[T]{}
	}
	return Gob[T]()
}

// Gob returns a codec encoding each value with encoding/gob, on its own.
// A value of an interface type needs its concrete types registered with
// gob.Register.
func Gob[T any]() Codec[T] { return gobCodec[T]{} }

var errTrailing = errors.New("kvs: trailing bytes")

type stringCodec struct{}

func (stringCodec) Append(b []byte, v string) ([]byte, error) { return append(b, v...), nil }
func (stringCodec) Decode(b []byte) (string, error)           { return string(b), nil }

type bytesCodec struct{}

func (bytesCodec) Append(b []byte, v []byte) ([]byte, error) { return append(b, v...), nil }
func (bytesCodec) Decode(b []byte) ([]byte, error)           { return bytes.Clone(b), nil }

type intCodec[T int | int8 | int16 | int32 | int64] struct{}

func (intCodec[T]) Append(b []byte, v T) ([]byte, error) {
	return binary.AppendVarint(b, int64(v)), nil
}

func (intCodec[T]) Decode(b []byte) (T, error) {
	v, n := binary.Varint(b)
	whole := n > 0 && n == len(b) // one varint, nothing after
	if !whole || int64(T(v)) != v {
		return 0, errTrailing
	}
	return T(v), nil
}

type uintCodec[T uint | uint8 | uint16 | uint32 | uint64] struct{}

func (uintCodec[T]) Append(b []byte, v T) ([]byte, error) {
	return binary.AppendUvarint(b, uint64(v)), nil
}

func (uintCodec[T]) Decode(b []byte) (T, error) {
	v, n := binary.Uvarint(b)
	whole := n > 0 && n == len(b) // one varint, nothing after
	if !whole || uint64(T(v)) != v {
		return 0, errTrailing
	}
	return T(v), nil
}

type binaryCodec[T any] struct{}

func (binaryCodec[T]) Append(b []byte, v T) ([]byte, error) {
	if a, ok := any(v).(encoding.BinaryAppender); ok {
		return a.AppendBinary(b)
	}
	m, err := any(v).(encoding.BinaryMarshaler).MarshalBinary()
	return append(b, m...), err
}

func (binaryCodec[T]) Decode(b []byte) (T, error) {
	var v T
	err := any(&v).(encoding.BinaryUnmarshaler).UnmarshalBinary(b)
	return v, err
}

type gobCodec[T any] struct{}

func (gobCodec[T]) Append(b []byte, v T) ([]byte, error) {
	w := bytes.NewBuffer(b)
	err := gob.NewEncoder(w).Encode(&v)
	return w.Bytes(), err
}

func (gobCodec[T]) Decode(b []byte) (T, error) {
	var v T
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&v)
	return v, err
}
