package gensim

import (
	"cmp"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/shun159/molecule/proc"
)

// A behaviour must not write the state it is given, nor a message once
// sent: the runtime, Terminate after a panic, and the holders of a
// message may still read them. A slice, map or pointer shared by the
// state given and the state returned is the usual way to break this, and
// it goes unnoticed, as the program still runs. The simulation checks it:
// the state a process is given, and a message in flight, are fingerprinted
// before and after, through their slices, maps and pointers, and must not
// change.

// ImpureError is what a simulation panics with when a behaviour writes the
// state it was given, or a message after sending it.
type ImpureError struct {
	PID  proc.PID // the process
	Msg  any      // what it was handling, or the message changed
	What string
}

func (e *ImpureError) Error() string {
	return fmt.Sprintf("gensim: %v %s (%T): copy a slice, map or pointer of it before changing it, with slices.Clone or maps.Clone",
		e.PID, e.What, e.Msg)
}

// WithoutPurityCheck turns the checks off, for speed.
func WithoutPurityCheck() Option { return func(s *Sim) { s.noPurity = true } }

// fingerprint writes what v holds and reaches through pointers, slices,
// maps and interfaces.
func fingerprint(v any) string {
	f := fingerprinter{seen: make(map[uintptr]int)}
	f.value(reflect.ValueOf(v))
	return f.b.String()
}

type fingerprinter struct {
	b    strings.Builder
	seen map[uintptr]int // pointers followed, numbered, for cycles and sharing
	// identity writes a pointer as its address, not followed: for the
	// keys of a map, to order them alike from a fingerprint to the next.
	identity bool
}

var locationType = reflect.TypeFor[time.Location]()

func (f *fingerprinter) value(v reflect.Value) {
	b := &f.b
	if !v.IsValid() {
		b.WriteString("nil")
		return
	}
	switch v.Kind() {
	case reflect.Bool:
		b.WriteString(strconv.FormatBool(v.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(v.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		b.WriteString(strconv.FormatUint(v.Uint(), 10))
	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(v.Float(), 'g', -1, 64))
	case reflect.Complex64, reflect.Complex128:
		b.WriteString(strconv.FormatComplex(v.Complex(), 'g', -1, 128))
	case reflect.String:
		b.WriteString(strconv.Quote(v.String()))
	case reflect.Pointer:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		// A location keeps a cache of its own: it is told by identity.
		if v.Type().Elem() == locationType {
			fmt.Fprintf(b, "loc%x", v.Pointer())
			return
		}
		if f.identity {
			fmt.Fprintf(b, "ptr%x", v.Pointer())
			return
		}
		if n, ok := f.seen[v.Pointer()]; ok {
			fmt.Fprintf(b, "@%d", n)
			return
		}
		f.seen[v.Pointer()] = len(f.seen)
		b.WriteByte('*')
		f.value(v.Elem())
	case reflect.Interface:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		b.WriteString(v.Elem().Type().String())
		f.value(v.Elem())
	case reflect.Struct:
		b.WriteByte('{')
		for i := range v.NumField() {
			f.value(v.Field(i))
			b.WriteByte(',')
		}
		b.WriteByte('}')
	case reflect.Slice:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		fallthrough
	case reflect.Array:
		b.WriteByte('[')
		for i := range v.Len() {
			f.value(v.Index(i))
			b.WriteByte(',')
		}
		b.WriteByte(']')
	case reflect.Map:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		// In an order of their own, not Go's, which changes: of a pointer
		// two entries share, the first followed is written, and the next
		// told by its number, as are the pointers met after.
		type key struct {
			v  reflect.Value
			id string
		}
		var keys []key
		for _, k := range v.MapKeys() {
			id := fingerprinter{identity: true}
			id.value(k)
			keys = append(keys, key{k, id.b.String()})
		}
		slices.SortFunc(keys, func(a, b key) int { return cmp.Compare(a.id, b.id) })
		b.WriteString("map[")
		for _, k := range keys {
			f.value(k.v)
			b.WriteByte(':')
			f.value(v.MapIndex(k.v))
			b.WriteByte(',')
		}
		b.WriteByte(']')
	default: // functions, channels: by identity
		fmt.Fprintf(b, "%s%x", v.Kind(), v.Pointer())
	}
}
