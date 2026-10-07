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
	// vectored tells a connection writing several buffers at once, as a
	// TCP connection does with writev. Another, such as TLS, gets a
	// packet in one write, joined in buf, rather than one per buffer.
	vectored bool
	buf      []byte
}

func newWriter(conn net.Conn, opts Options) *writer {
	_, tcp := conn.(*net.TCPConn)
	_, unix := conn.(*net.UnixConn)
	return &writer{conn: conn, packet: opts.Packet, timeout: opts.SendTimeout, vectored: tcp || unix}
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
	switch {
	case len(hdr) == 0:
		_, err = w.conn.Write(data)
	case w.vectored:
		bufs := net.Buffers{hdr, data}
		_, err = bufs.WriteTo(w.conn)
	default:
		w.buf = append(append(w.buf[:0], hdr...), data...)
		_, err = w.conn.Write(w.buf)
		if cap(w.buf) > 64<<10 {
			w.buf = nil // keep no large buffer
		}
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
