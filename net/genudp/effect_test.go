package genudp_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/net/genudp"
	"github.com/shun159/molecule/proc"
)

// upper is a genserver answering each datagram of the socket cast to it
// with the datagram in upper case, one at a time, and passing on to the
// process in its state what the socket tells it otherwise.
type upper struct{ to proc.PID }

func (u upper) Init(proc.PID) (struct{}, []molecule.Effect, error) { return struct{}{}, nil, nil }

func (upper) HandleCall(s struct{}, _ struct{}, _ genserver.From[struct{}]) (struct{}, []molecule.Effect) {
	return s, nil
}

func (upper) HandleCast(s struct{}, sock genudp.Socket) (struct{}, []molecule.Effect) {
	return s, molecule.Do(sock.SetActiveEffect(genudp.Once))
}

func (u upper) HandleInfo(s struct{}, msg any) (struct{}, []molecule.Effect) {
	switch m := msg.(type) {
	case genudp.DataMsg:
		if string(m.Bytes) == "huge" {
			return s, molecule.Do(m.Sock.SendActiveEffect(m.From, make([]byte, 70000), genudp.Once))
		}
		return s, molecule.Do(m.Sock.SendActiveEffect(m.From, bytes.ToUpper(m.Bytes), genudp.Once))
	case genudp.SendErrorMsg:
		return s, molecule.Do(molecule.Send{To: u.to, Msg: m})
	}
	return s, nil
}

func TestEffects(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	inProc(t, n, func(s *proc.Self) {
		srv, err := genserver.Start(ctx, n, upper{to: s.PID()})
		if err != nil {
			t.Fatal(err)
		}
		pid, _ := srv.Dest().WhereIs(n)
		sock, peer := open(t, s, genudp.Options{})
		if err := sock.ControllingProcess(ctx, s, pid); err != nil {
			t.Fatal(err)
		}
		srv.Cast(s, sock)

		buf := make([]byte, 16)
		for _, d := range []string{"one", "two"} {
			send(t, peer, sock, d)
			peer.SetReadDeadline(time.Now().Add(2 * time.Second))
			k, _, err := peer.ReadFromUDPAddrPort(buf)
			if err != nil || string(buf[:k]) != string(bytes.ToUpper([]byte(d))) {
				t.Fatalf("peer got %q, %v", buf[:k], err)
			}
		}

		// A send that fails comes back to the behaviour; the socket goes on.
		send(t, peer, sock, "huge")
		m, ok := receive(t, s).(genudp.SendErrorMsg)
		if !ok || m.To != peerAddr(peer) || m.Err == nil {
			t.Fatalf("got %#v, want SendErrorMsg", m)
		}
		send(t, peer, sock, "three")
		peer.SetReadDeadline(time.Now().Add(2 * time.Second))
		if k, _, err := peer.ReadFromUDPAddrPort(buf); err != nil || string(buf[:k]) != "THREE" {
			t.Fatalf("peer got %q, %v", buf[:k], err)
		}
	})
}
