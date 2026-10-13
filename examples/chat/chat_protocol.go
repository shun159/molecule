package main

import (
	"strings"

	"github.com/shun159/molecule/actor"
	"github.com/shun159/molecule/dist/pg"
	"github.com/shun159/molecule/net/gentcp"
	"github.com/shun159/molecule/proc"
)

// ChatProtocol is the gen_tcp_acceptor of one person: the first line they
// send is their nick, then each line goes to everyone in the lobby.
type ChatProtocol struct{}

// conn is the state of a connection.
type conn struct {
	self proc.PID
	nick string // empty until given
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

func (ChatProtocol) Init(self proc.PID, sock gentcp.Socket) (conn, []actor.Effect, error) {
	return conn{self: self}, actor.Do(sock.SendEffect([]byte("nick? "))), nil
}

// HandleData handles a line: the socket cuts them, see startChat.
func (p ChatProtocol) HandleData(c conn, sock gentcp.Socket, line []byte) (conn, []actor.Effect) {
	text := strings.TrimSpace(string(line))
	switch {
	case text == "":
		return c, nil
	case c.nick == "":
		c.nick = text
		return c, actor.Do(
			pg.JoinEffect(scope, lobby, c.self),
			pg.SendEffect(scope, lobby, said{Text: c.nick + " joined"}, proc.PID{}),
		)
	}
	return c, actor.Do(pg.SendEffect(scope, lobby, said{From: c.nick, Text: text}, proc.PID{}))
}

// HandleInfo writes what the lobby says.
func (ChatProtocol) HandleInfo(c conn, sock gentcp.Socket, msg any) (conn, []actor.Effect) {
	if s, ok := msg.(said); ok {
		return c, actor.Do(sock.SendEffect([]byte(s.String())))
	}
	return c, nil
}

// Terminate tells the others; the scope takes the process out of the
// lobby by itself once it is gone.
func (ChatProtocol) Terminate(c conn, _ error) []actor.Effect {
	if c.nick == "" {
		return nil
	}
	return actor.Do(pg.SendEffect(scope, lobby, said{Text: c.nick + " left"}, c.self))
}
