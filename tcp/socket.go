// Package tcp serves TCP connections with processes, like Erlang's Ranch:
// a pool of acceptor processes hands each connection to a handler process
// started under a dynamic supervisor.
//
// A connection is itself a process, as a port is in Erlang, so handlers
// stay pure: data arrives as messages, and writing is an effect.
package tcp

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// Messages a handler receives from its socket, as gen.InfoMsg.
type (
	// Attached gives the handler the socket process of its connection.
	// It is the first message from the socket.
	Attached struct {
		Sock proc.PID
	}
	// Data carries bytes read from the connection, one read for each
	// ActiveOnce.
	Data struct {
		Sock  proc.PID
		Bytes []byte
	}
	// Closed reports that the connection is closed. Err is nil when the
	// peer closed it.
	Closed struct {
		Sock proc.PID
		Err  error
	}
)

// Requests to a socket process.
type (
	activeOnce struct{}
	write      struct{ b []byte }
	closeReq   struct{}
	readFailed struct{ err error }
)

// ActiveOnce is the effect asking the socket to read once and deliver what
// it reads as Data, like {active, once} in Erlang. Reading only on demand
// keeps a fast peer from filling the handler's mailbox.
func ActiveOnce(sock proc.PID) gen.Effect { return gen.Send{To: sock, Msg: activeOnce{}} }

// Write is the effect writing b to the connection. A failed write closes
// the connection, reported as Closed.
func Write(sock proc.PID, b []byte) gen.Effect { return gen.Send{To: sock, Msg: write{b}} }

// Close is the effect closing the connection.
func Close(sock proc.PID) gen.Effect { return gen.Send{To: sock, Msg: closeReq{}} }

// readBufferSize is the most a single read delivers.
const readBufferSize = 32 << 10

// attach starts the socket process of conn for owner and tells owner.
func attach(parent *proc.Self, conn net.Conn, owner proc.PID) {
	sock := parent.Spawn(func(s *proc.Self) error { return runSocket(s, conn, owner) })
	parent.Send(owner, Attached{Sock: sock})
}

// runSocket owns conn for owner: it reads on demand, writes on request, and
// closes conn when either itself or owner terminates, whatever the reason.
func runSocket(s *proc.Self, conn net.Conn, owner proc.PID) error {
	// Closing the connection is also what unblocks a pending Read once the
	// process is dead.
	context.AfterFunc(s.Context(), func() { conn.Close() })
	defer conn.Close()
	ownerRef := s.Monitor(owner)

	n, self := s.Node(), s.PID()
	permits := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, readBufferSize)
		for {
			select {
			case <-permits:
			case <-s.Context().Done():
				return
			}
			k, err := conn.Read(buf)
			if k > 0 {
				n.Send(owner, Data{Sock: self, Bytes: append([]byte(nil), buf[:k]...)})
			}
			if err != nil {
				n.Send(self, readFailed{err})
				return
			}
		}
	}()

	for {
		msg, err := s.Receive(context.Background())
		if err != nil {
			return err
		}
		switch m := msg.(type) {
		case activeOnce:
			select {
			case permits <- struct{}{}:
			default: // already asked
			}
		case write:
			if _, err := conn.Write(m.b); err != nil {
				s.Send(owner, Closed{Sock: self, Err: err})
				return nil
			}
		case readFailed:
			err := m.err
			if errors.Is(err, io.EOF) {
				err = nil
			}
			s.Send(owner, Closed{Sock: self, Err: err})
			return nil
		case closeReq:
			return nil
		case proc.DownMsg:
			if m.Ref == ownerRef {
				return nil
			}
		}
	}
}
