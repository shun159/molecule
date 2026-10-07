package main

import (
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/proc"
)

// EchoProtocol handles one connection, like a Ranch protocol: a
// gen_tcp_acceptor behaviour writing back what it reads, and reporting to
// the stats server as it goes.
type EchoProtocol struct {
	stats genserver.Ref[GetStats, Stats, StatsEvent]
}

func (p EchoProtocol) Init(proc.PID, gentcpacceptor.Socket) (struct{}, []gen.Effect, error) {
	return struct{}{}, gen.Do(p.stats.CastEffect(connOpened{})), nil
}

func (p EchoProtocol) HandleData(s struct{}, sock gentcpacceptor.Socket, b []byte) (struct{}, []gen.Effect) {
	return s, gen.Do(sock.Write(b), p.stats.CastEffect(echoed{len(b)}))
}

func (p EchoProtocol) Terminate(struct{}, error) []gen.Effect {
	return gen.Do(p.stats.CastEffect(connClosed{}))
}
