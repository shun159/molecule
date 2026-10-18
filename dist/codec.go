package dist

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// Codec turns the messages of processes into bytes and back. A connection
// makes an Encoder and a Decoder of its own, and uses them in order: what
// one Encoder made, one Decoder reads, in the same order, so they may keep
// state from message to message.
type Codec interface {
	NewEncoder() Encoder
	NewDecoder() Decoder
}

// Encoder encodes messages. A message it fails to encode is dropped; the
// Encoder must be usable after.
type Encoder interface {
	Encode(msg any) ([]byte, error)
}

// Decoder decodes what the Encoder of the other side encoded.
type Decoder interface {
	Decode(b []byte) (any, error)
}

// Register tells the distribution the types of the messages sent between
// nodes, by a value of each, as the default Codec needs: it sends a
// message of a type it knows, with its exported fields. Every node
// registers the types it sends or receives, at init, before connecting.
// Built-in types, PIDs, Refs and the calls and casts of gen need not be
// registered.
//
//	func init() { dist.Register(Ping{}, Pong{}) }
//
// A type is described once per connection, the first time it is sent, or
// is sent as its own bytes, if binary: see AppendAny.
func Register(msgs ...any) {
	var ids []typeID
	for _, m := range msgs {
		gob.Register(m)
		t := reflect.TypeOf(m)
		ids = append(ids, typeID{t: t, id: hashName(t), empty: noFields(t), bin: methodsOf(t)})
	}
	register(ids...)
}

// register adds the types to those registered.
func register(ids ...typeID) {
	registering.Lock()
	defer registering.Unlock()
	r := registry{byType: map[reflect.Type]typeID{}, byID: map[uint64]typeID{}}
	if old := registered.Load(); old != nil {
		for t, id := range old.byType {
			r.byType[t], r.byID[id.id] = id, id
		}
	}
	for _, id := range ids {
		if other, ok := r.byID[id.id]; ok && other.t != id.t {
			panic(fmt.Sprintf("dist: %v and %v register under one ID", id.t, other.t))
		}
		r.byType[id.t], r.byID[id.id] = id, id
	}
	registered.Store(&r)
}

// The types registered, to send a message of one by an ID of its own,
// rather than its name, in an interface, which gob reads by reflection.
type (
	registry struct {
		byType map[reflect.Type]typeID
		byID   map[uint64]typeID
	}
	typeID struct {
		t     reflect.Type
		id    uint64       // a hash of its name, the same on every node
		empty bool         // without exported fields: gob encodes none of it
		bin   *binaryCodec // if binary, how, see AppendAny
	}
)

var (
	registering sync.Mutex
	registered  atomic.Pointer[registry]
)

func hashName(t reflect.Type) uint64 {
	name := t.String()
	if t.Name() != "" {
		name = t.PkgPath() + "." + t.Name()
	}
	h := fnv.New64a()
	h.Write([]byte(name))
	return h.Sum64()
}

func noFields(t reflect.Type) bool {
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := range t.NumField() {
		if t.Field(i).IsExported() {
			return false
		}
	}
	return true
}

// typeOf returns the type of msg, if registered.
func typeOf(msg any) (typeID, bool) {
	r := registered.Load()
	if r == nil {
		return typeID{}, false
	}
	id, ok := r.byType[reflect.TypeOf(msg)]
	return id, ok
}

func typeByID(id uint64) (typeID, bool) {
	r := registered.Load()
	if r == nil {
		return typeID{}, false
	}
	t, ok := r.byID[id]
	return t, ok
}

func init() {
	gob.Register(proc.PID{})
	gob.Register(proc.Ref{})
	gob.Register(actor.CallMsg{})
	gob.Register(actor.CastMsg{})
	gob.Register(actor.CallAbandoned{})
}

// gobCodec is the default Codec, of encoding/gob.
type gobCodec struct{}

// envelope carries a message in an interface, so that gob tells its type.
type envelope struct{ Msg any }

// A gob stream describes each type once, and an encoding that fails may
// leave the stream in a state the other side cannot follow: the encoder
// then starts a new stream, which the first byte of each message tells.
// A message of a type registered follows the ID of its type, as typed
// tells, and is its value alone; another, in an envelope. A message of a
// binary type is out of the stream: its first byte is binaryMsg, then the
// ID of its type, then its bytes.
const (
	sameStream byte = iota
	newStream

	typed     byte = 2
	binaryMsg byte = 4
)

func (gobCodec) NewEncoder() Encoder { return &gobEncoder{} }
func (gobCodec) NewDecoder() Decoder { return &gobDecoder{} }

type gobEncoder struct {
	buf bytes.Buffer
	enc *gob.Encoder
	bin []byte // to append a binary message to
}

func (e *gobEncoder) Encode(msg any) ([]byte, error) {
	t, ok := typeOf(msg)
	if ok && t.bin != nil {
		b, err := e.encodeBinary(t, msg)
		if !errors.Is(err, ErrNotBinary) {
			return b, err
		}
	}
	flag := e.start()
	var err error
	if ok {
		err = e.encodeTyped(flag, t, msg)
	} else {
		e.buf.WriteByte(flag)
		err = e.enc.Encode(envelope{msg})
	}
	if err != nil {
		e.enc = nil
		return nil, err
	}
	return bytes.Clone(e.buf.Bytes()), nil
}

// start empties the buffer for a message, and returns the flag of its
// stream: a new one, if the last encoding failed or none was yet.
func (e *gobEncoder) start() byte {
	e.buf.Reset()
	if e.enc != nil {
		return sameStream
	}
	e.enc = gob.NewEncoder(&e.buf)
	return newStream
}

// encodeTyped encodes msg, of the registered type t, after the ID of t:
// its value alone, or nothing if t has no fields.
func (e *gobEncoder) encodeTyped(flag byte, t typeID, msg any) error {
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], t.id)
	e.buf.WriteByte(flag | typed)
	e.buf.Write(id[:])
	if t.empty {
		return nil
	}
	return e.enc.Encode(msg)
}

// encodeBinary encodes msg, of the binary type t, as its own bytes.
func (e *gobEncoder) encodeBinary(t typeID, msg any) ([]byte, error) {
	b := binary.BigEndian.AppendUint64(append(e.bin[:0], binaryMsg), t.id)
	b, err := t.bin.append(b, msg)
	if cap(b) <= maxKept {
		e.bin = b
	}
	if err != nil {
		return nil, err
	}
	return bytes.Clone(b), nil
}

// maxKept is the most an encoder keeps to append binary messages to: a
// message larger is let go once encoded.
const maxKept = 64 << 10

type gobDecoder struct {
	buf bytes.Buffer
	dec *gob.Decoder
}

var errStream = errors.New("dist: gob message out of its stream")

func (d *gobDecoder) Decode(b []byte) (any, error) {
	if len(b) == 0 {
		return nil, errStream
	}
	flag := b[0]
	if flag == binaryMsg {
		return decodeBinary(b[1:])
	}
	switch flag &^ typed {
	case newStream:
		d.buf.Reset()
		d.dec = gob.NewDecoder(&d.buf)
	case sameStream:
		if d.dec == nil {
			return nil, errStream
		}
	default:
		return nil, errStream
	}
	b = b[1:]
	if flag&typed != 0 {
		return d.decodeTyped(b)
	}
	d.buf.Write(b)
	var env envelope
	if err := d.dec.Decode(&env); err != nil {
		d.dec = nil // the stream is lost until the next new one
		return nil, err
	}
	return env.Msg, nil
}

// decodeTyped decodes a message of a type registered, after its ID.
func (d *gobDecoder) decodeTyped(b []byte) (any, error) {
	if len(b) < 8 {
		return nil, errStream
	}
	t, ok := typeByID(binary.BigEndian.Uint64(b))
	if !ok {
		return nil, fmt.Errorf("dist: a message of a type not registered, %#x", binary.BigEndian.Uint64(b))
	}
	if t.empty {
		return reflect.Zero(t.t).Interface(), nil
	}
	d.buf.Write(b[8:])
	v := reflect.New(t.t)
	if err := d.dec.DecodeValue(v); err != nil {
		d.dec = nil
		return nil, err
	}
	return v.Elem().Interface(), nil
}

// decodeBinary decodes a message of a binary type, after its first byte.
func decodeBinary(b []byte) (any, error) {
	if len(b) < 8 {
		return nil, errStream
	}
	t, ok := typeByID(binary.BigEndian.Uint64(b))
	if !ok || t.bin == nil {
		return nil, fmt.Errorf("dist: a binary message of a type not registered, %#x", binary.BigEndian.Uint64(b))
	}
	return t.bin.decode(b[8:])
}
