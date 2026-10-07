package main

import (
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

// EchoProtocol is the gen_server of one connection. It gets its socket
// from the acceptor as a cast, then echoes each packet in {active, once}
// mode: a packet arrives as a message, is written back, and the next one
// is asked for, in one message to the socket. Everything it does is an effect, so it is tested by
// calling it.
type EchoProtocol struct{}

// connsName is the dynamic supervisor of the protocols.
var connsName = gen.Local("echo_conns")

func connsChildSpec(id string) supervisor.ChildSpec {
	return supervisor.ChildSpec{
		ID:    id,
		Type:  supervisor.Supervisor,
		Start: supervisor.StartDynamicLinkFunc(supervisor.DynamicSpec{Name: connsName}),
	}
}

func protocolChildSpec() supervisor.ChildSpec {
	return supervisor.ChildSpec{
		Restart: supervisor.Temporary,
		Start:   genserver.StartLinkFunc(EchoProtocol{}),
	}
}

func protocolRef(pid proc.PID) genserver.Ref[struct{}, struct{}, gentcp.Socket] {
	return genserver.RefFor(EchoProtocol{}, pid)
}

// conn is the state of a connection: whether it was counted as opened,
// and the bytes echoed, told to echo_stats once at the end rather than
// on every packet, which would have every connection message it.
type conn struct {
	open  bool
	bytes int
}

func (EchoProtocol) Init(proc.PID) (conn, []gen.Effect, error) { return conn{}, nil, nil }

func (EchoProtocol) HandleCall(c conn, _ struct{}, _ genserver.From[struct{}]) (conn, []gen.Effect) {
	return c, nil
}

// HandleCast takes the socket.
func (EchoProtocol) HandleCast(c conn, sock gentcp.Socket) (conn, []gen.Effect) {
	c.open = true
	return c, gen.Do(
		statsRef.CastEffect(connOpened{}),
		sock.SetActiveEffect(gentcp.Once),
	)
}

func (EchoProtocol) HandleInfo(c conn, msg any) (conn, []gen.Effect) {
	switch m := msg.(type) {
	case gentcp.DataMsg:
		c.bytes += len(m.Bytes)
		return c, gen.Do(m.Sock.SendActiveEffect(m.Bytes, gentcp.Once))
	case gentcp.ClosedMsg:
		return c, gen.Do(gen.Stop{})
	}
	return c, nil // an ErrorMsg comes before ClosedMsg
}

// Terminate reports the connection closed, however it ended.
func (EchoProtocol) Terminate(c conn, _ error) []gen.Effect {
	if !c.open {
		return nil
	}
	return gen.Do(statsRef.CastEffect(echoed{c.bytes}), statsRef.CastEffect(connClosed{}))
}
