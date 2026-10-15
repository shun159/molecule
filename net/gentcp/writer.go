package gentcp

import (
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/behaviours/gen"
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
	// TCP or a Unix connection does with writev, a syscall.Conn, wrapped
	// or not. Another, such as TLS, gets a packet in one write, joined in
	// buf, rather than one per buffer.
	vectored bool
	buf      []byte
	hdrs     []byte      // the prefixes of the packets of a write
	bufs     net.Buffers // the buffers of a vectored write
}

func newWriter(conn net.Conn, opts Options) *writer {
	_, vectored := conn.(syscall.Conn)
	return &writer{conn: conn, packet: opts.Packet, timeout: opts.SendTimeout, vectored: vectored}
}

// sendFailed tells the socket process that a write failed.
type sendFailed struct{ err error }

// write writes packets, in one write if it can. A failure of the
// connection is to be told to the socket process, which then fails it:
// the stream is cut at an unknown place, and of no use anymore.
func (w *writer) write(packets ...[]byte) (failed bool, err error) {
	for _, data := range packets {
		if _, err := w.packet.header(len(data)); err != nil {
			return false, err
		}
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
	w.hdrs = w.hdrs[:0]
	for _, data := range packets {
		hdr, _ := w.packet.header(len(data))
		w.hdrs = append(w.hdrs, hdr...)
	}
	h := w.packet.prefix()
	switch {
	case h == 0 && len(packets) == 1:
		_, err = w.conn.Write(packets[0])
	case w.vectored:
		w.bufs = w.bufs[:0]
		for i, data := range packets {
			w.bufs = append(w.bufs, w.hdrs[i*h:(i+1)*h], data)
		}
		bufs := w.bufs // WriteTo consumes what it is given
		_, err = bufs.WriteTo(w.conn)
		clear(w.bufs) // keep no data
	default:
		w.buf = w.buf[:0]
		for i, data := range packets {
			w.buf = append(append(w.buf, w.hdrs[i*h:(i+1)*h]...), data...)
		}
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
	actor.Extension
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
