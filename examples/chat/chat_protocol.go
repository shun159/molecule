package main

import (
	"bytes"
	"slices"
	"strings"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/pg"
	"github.com/shun159/molecule/proc"
)

// ChatProtocol is the gen_tcp_acceptor of one person: the first line they
// send is their nick, then each line goes to everyone in the lobby.
type ChatProtocol struct{}

// conn is the state of a connection.
type conn struct {
	self proc.PID
	nick string // empty until given
	buf  []byte // the start of a line not ended yet
}

// said is what goes to the lobby: a line of someone, or of the server when
// From is empty.
type said struct {
	From string
	Text string
}

func (s said) String() string {
	if s.From == "" {
		return "* " + s.Text + "\n"
	}
	return "<" + s.From + "> " + s.Text + "\n"
}

func (ChatProtocol) Init(self proc.PID, sock gentcpacceptor.Socket) (conn, []gen.Effect, error) {
	return conn{self: self}, gen.Do(sock.Write([]byte("nick? "))), nil
}

func (p ChatProtocol) HandleData(c conn, sock gentcpacceptor.Socket, data []byte) (conn, []gen.Effect) {
	lines, rest := splitLines(append(slices.Clip(c.buf), data...))
	c.buf = rest
	var effs []gen.Effect
	for _, line := range lines {
		if line == "" {
			continue
		}
		if c.nick == "" {
			c.nick = line
			effs = append(effs,
				pg.JoinEffect(scope, lobby, c.self),
				pg.SendEffect(scope, lobby, said{Text: c.nick + " joined"}, proc.PID{}),
			)
			continue
		}
		effs = append(effs, pg.SendEffect(scope, lobby, said{From: c.nick, Text: line}, proc.PID{}))
	}
	return c, effs
}

// HandleInfo writes what the lobby says.
func (ChatProtocol) HandleInfo(c conn, sock gentcpacceptor.Socket, msg any) (conn, []gen.Effect) {
	if s, ok := msg.(said); ok {
		return c, gen.Do(sock.Write([]byte(s.String())))
	}
	return c, nil
}

// Terminate tells the others; the scope takes the process out of the
// lobby by itself once it is gone.
func (ChatProtocol) Terminate(c conn, _ error) []gen.Effect {
	if c.nick == "" {
		return nil
	}
	return gen.Do(pg.SendEffect(scope, lobby, said{Text: c.nick + " left"}, c.self))
}

// splitLines returns the complete lines in b, without their line ends,
// and what follows the last one.
func splitLines(b []byte) (lines []string, rest []byte) {
	for {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			return lines, b
		}
		lines = append(lines, strings.TrimSpace(string(b[:i])))
		b = b[i+1:]
	}
}
