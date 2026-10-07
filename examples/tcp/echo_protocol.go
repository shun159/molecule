package main

import (
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
	"github.com/shun159/molecule/tcp"
)

// EchoProtocol is the gen_server handling one connection, like a Ranch
// protocol: the socket talks to it with messages (handle_info), and it
// writes back through effects. It takes no calls or casts of its own.
type EchoProtocol struct {
	stats genserver.Ref[GetStats, Stats, StatsEvent]
}

func protocolStartFunc(stats genserver.Ref[GetStats, Stats, StatsEvent]) supervisor.StartFunc {
	return genserver.StartLinkFunc(EchoProtocol{stats: stats})
}

// The state is the socket, known once the connection is attached.
func (p EchoProtocol) Init() (proc.PID, []gen.Effect, error) {
	return proc.PID{}, gen.Do(p.stats.CastEffect(connOpened{})), nil
}

func (EchoProtocol) HandleCall(sock proc.PID, _ struct{}, _ genserver.From[struct{}]) (proc.PID, []gen.Effect) {
	return sock, nil
}

func (EchoProtocol) HandleCast(sock proc.PID, _ struct{}) (proc.PID, []gen.Effect) {
	return sock, nil
}

func (p EchoProtocol) HandleInfo(sock proc.PID, msg any) (proc.PID, []gen.Effect) {
	switch m := msg.(type) {
	case tcp.Attached:
		return m.Sock, gen.Do(tcp.ActiveOnce(m.Sock))
	case tcp.Data:
		return sock, gen.Do(
			tcp.Write(sock, m.Bytes),
			p.stats.CastEffect(echoed{len(m.Bytes)}),
			tcp.ActiveOnce(sock),
		)
	case tcp.Closed:
		return sock, gen.Do(gen.Stop{})
	}
	return sock, nil
}

func (p EchoProtocol) Terminate(proc.PID, error) []gen.Effect {
	return gen.Do(p.stats.CastEffect(connClosed{}))
}
