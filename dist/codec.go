package dist

import (
	"bytes"
	"encoding/gob"
	"errors"

	"github.com/shun159/molecule"
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
// A type is described once per connection, the first time it is sent.
func Register(msgs ...any) {
	for _, m := range msgs {
		gob.Register(m)
	}
}

func init() {
	gob.Register(proc.PID{})
	gob.Register(proc.Ref{})
	gob.Register(molecule.CallMsg{})
	gob.Register(molecule.CastMsg{})
}

// gobCodec is the default Codec, of encoding/gob.
type gobCodec struct{}

// envelope carries a message in an interface, so that gob tells its type.
type envelope struct{ Msg any }

// A gob stream describes each type once, and an encoding that fails may
// leave the stream in a state the other side cannot follow: the encoder
// then starts a new stream, which the first byte of each message tells.
const (
	sameStream byte = iota
	newStream
)

func (gobCodec) NewEncoder() Encoder { return &gobEncoder{} }
func (gobCodec) NewDecoder() Decoder { return &gobDecoder{} }

type gobEncoder struct {
	buf bytes.Buffer
	enc *gob.Encoder
}

func (e *gobEncoder) Encode(msg any) ([]byte, error) {
	e.buf.Reset()
	flag := sameStream
	if e.enc == nil {
		e.enc = gob.NewEncoder(&e.buf)
		flag = newStream
	}
	e.buf.WriteByte(flag)
	if err := e.enc.Encode(envelope{msg}); err != nil {
		e.enc = nil
		return nil, err
	}
	return bytes.Clone(e.buf.Bytes()), nil
}

type gobDecoder struct {
	buf bytes.Buffer
	dec *gob.Decoder
}

var errStream = errors.New("dist: gob message out of its stream")

func (d *gobDecoder) Decode(b []byte) (any, error) {
	if len(b) == 0 {
		return nil, errStream
	}
	switch b[0] {
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
	d.buf.Write(b[1:])
	var env envelope
	if err := d.dec.Decode(&env); err != nil {
		d.dec = nil // the stream is lost until the next new one
		return nil, err
	}
	return env.Msg, nil
}
