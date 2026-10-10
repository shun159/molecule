// Package dgram is the core of the datagram sockets owned by processes,
// genudp's and genraw's: the socket process, reading only while its owner
// wants a datagram and handing each over itself, and the calls and effects
// acting on it. A package using it brings the connection, its address type,
// and the messages and errors it gives its owner.
package dgram

import (
	"context"
	"errors"
	"net"
	"os"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/gen"
	"github.com/shun159/molecule/proc"
)

// Conn is a datagram socket, its addresses of type A. Its methods may be
// called from several goroutines at once, and Close wakes a ReadFrom
// waiting.
type Conn[A any] interface {
	ReadFrom(buf []byte) (n int, from A, err error)
	WriteTo(b []byte, to A) error
	Close() error
}

// Kind is what a package makes of the core: the messages its socket S
// sends its owner, and its errors.
type Kind[A, S any] struct {
	Name string // in labels and errors

	Data      func(s S, from A, b []byte) any
	Error     func(s S, err error) any
	Closed    func(s S) any
	Passive   func(s S) any
	SendError func(s S, to A, err error) any

	ErrClosed, ErrNotOwner, ErrActive, ErrTimeout error
}

// Requests to a socket process.
type (
	SendReq[A any] struct {
		To     A
		Data   []byte
		Then   bool // set active after
		Active Active
	}
	RecvReq      struct{ Timeout time.Duration } // 0: none
	SetActiveReq struct{ Active Active }
	ControlReq   struct{ Owner proc.PID }
	CloseReq     struct{}
)

// RecvRep is the reply to a Recv.
type RecvRep[A any] struct {
	From A
	Data []byte
	Err  error
}

// Write writes one datagram to to on conn, an error of conn closed being
// k's ErrClosed. Datagram writes are whole and safe from several goroutines
// at once, so it needs no lock.
func Write[A, S any](k *Kind[A, S], conn Conn[A], to A, data []byte) error {
	err := conn.WriteTo(data, to)
	if errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrClosed) {
		return k.ErrClosed
	}
	return err
}

// Call calls the socket process pid with req, and returns the error it
// replies with, or k's ErrClosed if it is gone.
func Call[A, S any](ctx context.Context, k *Kind[A, S], caller molecule.Caller, pid proc.PID, req any) error {
	v, err := molecule.Call(ctx, caller, pid, req)
	if err != nil {
		return gone(k, err)
	}
	err, _ = v.(error)
	return err
}

// Recv takes the next datagram of the passive socket pid, waiting until
// the deadline of ctx, if any.
func Recv[A, S any](ctx context.Context, k *Kind[A, S], caller molecule.Caller, pid proc.PID) (A, []byte, error) {
	var zero A
	var timeout time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		if timeout = time.Until(deadline); timeout <= 0 {
			return zero, nil, k.ErrTimeout
		}
	}
	// The socket ends the wait: giving up here could lose a datagram it
	// has just taken for this call.
	v, err := molecule.Call(context.Background(), caller, pid, RecvReq{timeout})
	if err != nil {
		return zero, nil, gone(k, err)
	}
	switch r := v.(type) {
	case RecvRep[A]:
		return r.From, r.Data, r.Err
	case error:
		return zero, nil, r
	}
	return zero, nil, nil
}

// gone turns the error of a call to a socket whose process is gone into
// ErrClosed.
func gone[A, S any](k *Kind[A, S], err error) error {
	var exit *molecule.ExitError
	if errors.As(err, &exit) {
		return k.ErrClosed
	}
	return err
}

// SendEffect is the effect sending data to to through the socket sock, for
// a behaviour, then setting its active mode if then. With conn, the
// runtime of the behaviour writes, and a failure comes back to it as k's
// SendError; without, the socket process does, and a failure is not told.
func SendEffect[A, S any](k *Kind[A, S], sock S, pid proc.PID, conn Conn[A], to A, data []byte, then bool, a Active) molecule.Effect {
	if conn == nil {
		return molecule.Send{To: pid, Msg: SendReq[A]{To: to, Data: data, Then: then, Active: a}}
	}
	return sendEffect[A, S]{k: k, sock: sock, pid: pid, conn: conn, to: to, data: data, then: then, active: a}
}

type sendEffect[A, S any] struct {
	molecule.Extension
	k      *Kind[A, S]
	sock   S
	pid    proc.PID
	conn   Conn[A]
	to     A
	data   []byte
	then   bool
	active Active
}

func (e sendEffect[A, S]) Perform(env gen.Env) {
	if err := Write(e.k, e.conn, e.to, e.data); err != nil {
		env.Send(env.Self(), e.k.SendError(e.sock, e.to, err))
	}
	if e.then {
		env.Send(e.pid, SetActiveReq{e.active})
	}
}
