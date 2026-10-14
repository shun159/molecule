package dist

import (
	"net"
	"sync/atomic"
)

// counted returns conn counting its reads that got bytes, for the
// connection to hear the node amid a frame. A TCP connection stays one,
// for the socket to write buffers at once, as net.Buffers does.
func counted(conn net.Conn) (net.Conn, *atomic.Uint64) {
	reads := new(atomic.Uint64)
	if tcp, ok := conn.(*net.TCPConn); ok {
		return countedTCP{tcp, reads}, reads
	}
	return countedConn{conn, reads}, reads
}

type countedTCP struct {
	*net.TCPConn
	reads *atomic.Uint64
}

func (c countedTCP) Read(b []byte) (int, error) {
	n, err := c.TCPConn.Read(b)
	if n > 0 {
		c.reads.Add(1)
	}
	return n, err
}

type countedConn struct {
	net.Conn
	reads *atomic.Uint64
}

func (c countedConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		c.reads.Add(1)
	}
	return n, err
}
