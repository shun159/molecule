package main

import (
	"io"
	"net"

	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/proc"
)

// echoProtocol returns the handler of one connection, like a Ranch
// protocol owning its socket: it writes back what it reads, and tells the
// stats server when the connection opens, and when it closes, with how
// many bytes it echoed.
//
// It is plain Go: io.Copy moves the data without going through any
// message, in the kernel where it can. The process around it is what the
// supervision tree sees.
func echoProtocol(stats genserver.Ref[GetStats, Stats, StatsEvent]) gentcpacceptor.Handler {
	return func(self *proc.Self, conn net.Conn) error {
		stats.Cast(self, connOpened{})
		n, err := io.Copy(conn, conn)
		stats.Cast(self, echoed{int(n)})
		stats.Cast(self, connClosed{})
		return err
	}
}
