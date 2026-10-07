package dist

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/shun159/molecule/proc"
)

// What goes between nodes, one frame per Packet4 packet: an operation,
// its fields, and for messages, the bytes of the Codec.
type op byte

const (
	opSend op = iota + 1
	opSendName
	opSendAlias
	opExit
	opExitLink
	opLink
	opUnlink
	opMonitor
	opDemonitor
	opDown
	opTick // nothing, but that the connection lives
)

// frame is an operation to or from another node.
type frame struct {
	op       op
	from, to proc.PID // Exit, Link, Unlink; Send: to; Monitor, Demonitor, Down: to is the target
	ref      proc.Ref // SendAlias, Monitor, Demonitor, Down
	name     string   // SendName
	reason   error    // Exit, Down
	msg      any      // Send, SendName, SendAlias: the message, before encoding
	payload  []byte   // the message, encoded
}

var errFrame = errors.New("dist: bad frame")

// appendFrame encodes f, its message already encoded in payload.
func appendFrame(b []byte, f frame) []byte {
	b = append(b, byte(f.op))
	switch f.op {
	case opSend:
		b = appendID(b, f.to)
	case opSendName:
		b = appendString(b, f.name)
	case opSendAlias:
		b = appendID(b, f.ref)
	case opExit, opExitLink:
		b = appendReason(appendID(appendID(b, f.from), f.to), f.reason)
	case opLink, opUnlink:
		b = appendID(appendID(b, f.from), f.to)
	case opMonitor, opDemonitor:
		b = appendID(appendID(b, f.ref), f.to)
	case opDown:
		b = appendReason(appendID(appendID(b, f.ref), f.to), f.reason)
	}
	return append(b, f.payload...)
}

func parseFrame(b []byte) (f frame, err error) {
	if len(b) == 0 {
		return f, errFrame
	}
	f.op, b = op(b[0]), b[1:]
	r := reader{b: b}
	switch f.op {
	case opSend:
		r.id(&f.to)
	case opSendName:
		f.name = r.string()
	case opSendAlias:
		r.id(&f.ref)
	case opExit, opExitLink:
		r.id(&f.from)
		r.id(&f.to)
		f.reason = r.reason()
	case opLink, opUnlink:
		r.id(&f.from)
		r.id(&f.to)
	case opMonitor, opDemonitor:
		r.id(&f.ref)
		r.id(&f.to)
	case opDown:
		r.id(&f.ref)
		r.id(&f.to)
		f.reason = r.reason()
	case opTick:
	default:
		return f, errFrame
	}
	if r.err != nil {
		return f, r.err
	}
	f.payload = r.b
	return f, nil
}

type marshaler interface{ MarshalBinary() ([]byte, error) }

func appendID(b []byte, id marshaler) []byte {
	enc, _ := id.MarshalBinary()
	return appendBytes(b, enc)
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func appendBytes(b, s []byte) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

// reader reads the fields of a frame, keeping the first error.
type reader struct {
	b   []byte
	err error
}

func (r *reader) bytes() []byte {
	if r.err != nil {
		return nil
	}
	if len(r.b) < 2 {
		r.err = errFrame
		return nil
	}
	l := int(binary.BigEndian.Uint16(r.b))
	if len(r.b) < 2+l {
		r.err = errFrame
		return nil
	}
	s := r.b[2 : 2+l]
	r.b = r.b[2+l:]
	return s
}

func (r *reader) string() string { return string(r.bytes()) }

func (r *reader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.b) < 1 {
		r.err = errFrame
		return 0
	}
	c := r.b[0]
	r.b = r.b[1:]
	return c
}

func (r *reader) id(id interface{ UnmarshalBinary([]byte) error }) {
	b := r.bytes()
	if r.err == nil {
		r.err = id.UnmarshalBinary(b)
	}
}

// Exit reasons travel as one of the reasons of proc, or as text: an error
// of the remote node is no Go value here. The text keeps what it wrapped
// of proc's reasons, so that errors.Is(reason, proc.Shutdown) holds across
// nodes.

// RemoteError is the exit reason of a process of another node, other than
// the reasons of proc: its text, and the reason of proc it wrapped, if
// any.
type RemoteError struct {
	Text  string
	Wraps error // proc.Normal, proc.Shutdown, or nil
}

func (e *RemoteError) Error() string { return e.Text }
func (e *RemoteError) Unwrap() error { return e.Wraps }

// The reasons of proc, by their code; 0 is any other.
var reasons = []error{nil, proc.Normal, proc.Shutdown, proc.Kill, proc.Killed, proc.NoProc, proc.NoConnection}

func reasonCode(err error) byte {
	for i, r := range reasons[1:] {
		if err == r {
			return byte(i + 1)
		}
	}
	return 0
}

func appendReason(b []byte, err error) []byte {
	if err == nil {
		err = proc.Normal
	}
	if c := reasonCode(err); c != 0 {
		return append(b, c)
	}
	wraps := byte(0)
	switch {
	case errors.Is(err, proc.Shutdown):
		wraps = reasonCode(proc.Shutdown)
	case errors.Is(err, proc.Normal):
		wraps = reasonCode(proc.Normal)
	}
	b = append(b, 0, wraps)
	return appendString(b, truncate(err.Error()))
}

func truncate(s string) string {
	if len(s) > 1<<15 {
		return s[:1<<15]
	}
	return s
}

func (r *reader) reason() error {
	c := r.byte()
	if r.err != nil {
		return nil
	}
	if c != 0 {
		if int(c) >= len(reasons) {
			r.err = fmt.Errorf("%w: reason %d", errFrame, c)
			return nil
		}
		return reasons[c]
	}
	w := r.byte()
	text := r.string()
	if int(w) >= len(reasons) {
		r.err = errFrame
		return nil
	}
	return &RemoteError{Text: text, Wraps: reasons[w]}
}
