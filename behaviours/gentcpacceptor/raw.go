package gentcpacceptor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/shun159/molecule/proc"
)

// Handler handles one connection as plain Go code, rather than as a
// Behaviour: it runs in the process of the connection, owns conn, reads
// and writes it directly, and returns when done. The connection is closed
// then, and also when the process dies, which ends a Read or Write it is
// blocked in.
//
// It suits connections whose data needs little handling, such as proxies
// and streams, where the messages of a Behaviour cost too much:
//
//	func echo(self *proc.Self, conn net.Conn) error {
//		_, err := io.Copy(conn, conn)
//		return err
//	}
//
// An error of the connection, such as a reset by the peer, ends the
// process with the error wrapped in proc.Shutdown, which is not reported.
// Other errors, and panics, are crashes, reported as such.
type Handler func(self *proc.Self, conn net.Conn) error

// NewRawListener returns a listener running h on each connection, not yet
// started.
func NewRawListener(spec Spec, h Handler) *Listener {
	return newListener(spec, func(ctx context.Context, parent *proc.Self, conn net.Conn) (proc.PID, error) {
		return parent.StartLink(ctx, func(s *proc.Self) error {
			context.AfterFunc(s.Context(), func() { conn.Close() })
			defer conn.Close()
			s.InitAck(nil)
			return connExit(h(s, conn))
		})
	})
}

// StartRaw starts a listener running h on each connection, on its own.
func StartRaw(ctx context.Context, n *proc.Node, spec Spec, h Handler) (*Listener, error) {
	return start(ctx, n, NewRawListener(spec, h))
}

// connExit is the exit reason of a handler that returned err: errors of
// the connection are no failure of the handler.
func connExit(err error) error {
	var ne net.Error
	switch {
	case err == nil:
		return nil
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed), errors.As(err, &ne):
		return fmt.Errorf("%w: %w", proc.Shutdown, err)
	}
	return err
}
