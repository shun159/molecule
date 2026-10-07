package gentcp

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// writer writes to the connection of a socket, in the process sending,
// as gen_tcp_socket does: sending costs no hop through the socket
// process. The lock keeps writes whole and in order; a socket whose
// process is dead writes nothing more.
type writer struct {
	mu      sync.Mutex
	conn    net.Conn
	packet  Packet
	timeout time.Duration
	done    <-chan struct{} // the socket process is dead
}

// sendFailed tells the socket process that a write failed.
type sendFailed struct{ err error }

// write writes a packet of data. A failure of the connection is to be
// told to the socket process, which then fails it: the stream is cut at
// an unknown place, and of no use anymore.
func (w *writer) write(data []byte) (failed bool, err error) {
	hdr, err := w.packet.header(len(data))
	if err != nil {
		return false, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.done:
		return false, ErrClosed
	default:
	}
	if w.timeout > 0 {
		w.conn.SetWriteDeadline(time.Now().Add(w.timeout))
	}
	if len(hdr) == 0 {
		_, err = w.conn.Write(data)
	} else {
		bufs := net.Buffers{hdr, data}
		_, err = bufs.WriteTo(w.conn)
	}
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, net.ErrClosed):
		return false, ErrClosed
	}
	return true, err
}

// sendEffect sends from the process of a behaviour, as Socket.Send does,
// then changes the active mode if then.
type sendEffect struct {
	gen.Extension
	pid    proc.PID
	w      *writer
	data   []byte
	then   bool
	active Active
}

func (e sendEffect) Perform(env gen.Env) {
	if failed, err := e.w.write(e.data); failed {
		env.Send(e.pid, sendFailed{err})
	}
	if e.then {
		env.Send(e.pid, setActiveReq{e.active})
	}
}
