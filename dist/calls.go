package dist

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/proc"
)

// The calls and casts of gen go as binary messages, their requests by
// AppendAny: one with a request AppendAny cannot encode goes by gob, as
// before. A reply is the value itself, binary if its type is.
func init() {
	register(
		builtin(actor.CallMsg{}, appendCall, decodeCall),
		builtin(actor.CastMsg{}, appendCast, decodeCast),
		builtin(actor.CallAbandoned{}, appendAbandoned, decodeAbandoned),
	)
}

// builtin is a type of the messages of gen, binary by the functions given.
func builtin(m any, append func([]byte, any) ([]byte, error), decode func([]byte) (any, error)) typeID {
	t := reflect.TypeOf(m)
	return typeID{t: t, id: hashName(t), bin: &binaryCodec{append: append, decode: decode}}
}

func appendCall(b []byte, v any) ([]byte, error) {
	m := v.(actor.CallMsg)
	return AppendAny(appendFrom(b, m.From), m.Req)
}

func decodeCall(b []byte) (any, error) {
	var m actor.CallMsg
	var err error
	if m.From, b, err = readFrom(b); err != nil {
		return nil, err
	}
	if m.Req, b, err = ReadAny(b); err != nil {
		return nil, err
	}
	return m, noneLeft(b)
}

func appendCast(b []byte, v any) ([]byte, error) {
	return AppendAny(b, v.(actor.CastMsg).Req)
}

func decodeCast(b []byte) (any, error) {
	req, b, err := ReadAny(b)
	if err != nil {
		return nil, err
	}
	return actor.CastMsg{Req: req}, noneLeft(b)
}

func appendAbandoned(b []byte, v any) ([]byte, error) {
	return appendFrom(b, v.(actor.CallAbandoned).From), nil
}

func decodeAbandoned(b []byte) (any, error) {
	from, b, err := readFrom(b)
	if err != nil {
		return nil, err
	}
	return actor.CallAbandoned{From: from}, noneLeft(b)
}

// appendFrom appends the caller of a call: its PID and its tag.
func appendFrom(b []byte, f actor.From) []byte {
	b, _ = AppendAny(b, f.PID)
	b, _ = AppendAny(b, f.Tag)
	return b
}

var errFrom = errors.New("dist: a call from what is not a PID and a Ref")

func readFrom(b []byte) (actor.From, []byte, error) {
	pid, b, err := ReadAny(b)
	if err != nil {
		return actor.From{}, b, err
	}
	tag, b, err := ReadAny(b)
	if err != nil {
		return actor.From{}, b, err
	}
	p, okPID := pid.(proc.PID)
	r, okTag := tag.(proc.Ref)
	if !okPID || !okTag {
		return actor.From{}, b, errFrom
	}
	return actor.From{PID: p, Tag: r}, b, nil
}

// noneLeft tells whether bytes are left after a message.
func noneLeft(b []byte) error {
	if len(b) > 0 {
		return fmt.Errorf("dist: %d bytes left of a binary message", len(b))
	}
	return nil
}
