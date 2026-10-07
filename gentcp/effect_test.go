package gentcp_test

import (
	"context"
	"io"
	"testing"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
)

// echo is a genserver echoing the lines of the socket cast to it, one at
// a time.
type echo struct{}

func (echo) Init(proc.PID) (struct{}, []gen.Effect, error) { return struct{}{}, nil, nil }

func (echo) HandleCall(s struct{}, _ struct{}, _ genserver.From[struct{}]) (struct{}, []gen.Effect) {
	return s, nil
}

func (echo) HandleCast(s struct{}, sock gentcp.Socket) (struct{}, []gen.Effect) {
	return s, []gen.Effect{sock.SetActiveEffect(gentcp.Once)}
}

func (echo) HandleInfo(s struct{}, msg any) (struct{}, []gen.Effect) {
	switch m := msg.(type) {
	case gentcp.DataMsg:
		if string(m.Bytes) == "bye\n" {
			return s, []gen.Effect{m.Sock.CloseEffect()}
		}
		return s, []gen.Effect{m.Sock.SendEffect(m.Bytes), m.Sock.SetActiveEffect(gentcp.Once)}
	case gentcp.ClosedMsg:
		return s, []gen.Effect{gen.Stop{}}
	}
	return s, nil
}

func TestEffects(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	srv, err := genserver.Start(ctx, n, echo{})
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := srv.Dest().WhereIs(n)
	inProc(t, n, func(s *proc.Self) {
		sock, peer := pair(t, s, gentcp.Options{Packet: gentcp.Line})
		if err := sock.ControllingProcess(ctx, s, pid); err != nil {
			t.Error(err)
		}
		srv.Cast(s, sock)
		io.WriteString(peer, "one\ntwo\n")
		if got := readAll(t, peer, 8); got != "one\ntwo\n" {
			t.Errorf("peer got %q", got)
		}
		io.WriteString(peer, "bye\n")
		wantEOF(t, peer)
		awaitExit(t, s, sock)
	})
}
