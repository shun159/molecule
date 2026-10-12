package kvs

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"unsafe"
)

// Type is the type of a table, as that of an ETS table: Set, a table of
// keys in no order, or OrderedSet, of keys in order.
type Type int

const (
	// Set holds the entries in a map, or a hash trie, in no order.
	Set Type = iota
	// OrderedSet holds the entries in a B-tree, in the order of their
	// keys: All goes in that order, and Range goes over an interval.
	OrderedSet
)

func (t Type) String() string {
	switch t {
	case Set:
		return "Set"
	case OrderedSet:
		return "OrderedSet"
	}
	return fmt.Sprintf("Type(%d)", int(t))
}

// compareOf returns the order of the keys of type K: by their method
// Compare(K) int, if they have one, as netip.Addr and time.Time do, or
// as cmp.Compare orders their strings or numbers, or bytes.Compare their
// arrays of bytes.
func compareOf[K comparable]() (func(a, b K) int, bool) {
	t := reflect.TypeFor[K]()
	if m, ok := t.MethodByName("Compare"); ok {
		if f, ok := m.Func.Interface().(func(a, b K) int); ok {
			return f, true
		}
	}
	switch t.Kind() {
	case reflect.String:
		return as[string, K](), true
	case reflect.Int:
		return as[int, K](), true
	case reflect.Int8:
		return as[int8, K](), true
	case reflect.Int16:
		return as[int16, K](), true
	case reflect.Int32:
		return as[int32, K](), true
	case reflect.Int64:
		return as[int64, K](), true
	case reflect.Uint:
		return as[uint, K](), true
	case reflect.Uint8:
		return as[uint8, K](), true
	case reflect.Uint16:
		return as[uint16, K](), true
	case reflect.Uint32:
		return as[uint32, K](), true
	case reflect.Uint64:
		return as[uint64, K](), true
	case reflect.Uintptr:
		return as[uintptr, K](), true
	case reflect.Float32:
		return as[float32, K](), true
	case reflect.Float64:
		return as[float64, K](), true
	case reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			n := t.Len()
			return func(a, b K) int {
				return bytes.Compare(unsafe.Slice((*byte)(unsafe.Pointer(&a)), n), unsafe.Slice((*byte)(unsafe.Pointer(&b)), n))
			}, true
		}
	}
	return nil, false
}

// as orders keys of type K, whose underlying type is T, as T.
func as[T cmp.Ordered, K any]() func(a, b K) int {
	return func(a, b K) int {
		return cmp.Compare(*(*T)(unsafe.Pointer(&a)), *(*T)(unsafe.Pointer(&b)))
	}
}
