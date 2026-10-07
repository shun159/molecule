package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

func TestEchoApplication(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	app, err := startEcho(ctx, n, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Stop(ctx, n, app.sup)

	conn, err := net.Dial("tcp", app.addr.String())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	for _, line := range []string{"hello", "world"} {
		fmt.Fprintln(conn, line)
		if got, err := r.ReadString('\n'); err != nil || got != line+"\n" {
			t.Fatalf("echo of %q = %q, %v", line, got, err)
		}
	}
	conn.Close()

	want := Stats{Open: 0, Total: 1, Bytes: int64(len("hello\nworld\n"))}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, err := statsRef.Call(ctx, n, GetStats{})
		if err == nil && s == want {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats = %+v, %v; want %+v", s, err, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// The gen_servers are pure, so their logic is tested without processes.
func TestStatsPure(t *testing.T) {
	s := Stats{}
	for _, ev := range []StatsEvent{connOpened{}, echoed{5}, connOpened{}, connClosed{}, echoed{3}} {
		s, _ = EchoStats{}.HandleCast(s, ev)
	}
	if want := (Stats{Open: 1, Total: 2, Bytes: 8}); s != want {
		t.Errorf("stats = %+v, want %+v", s, want)
	}
}

func TestProtocolPure(t *testing.T) {
	p := EchoProtocol{}
	sock := gentcp.Socket{PID: proc.NewNode("").NewPID()}
	c, _, _ := p.Init(proc.PID{})
	c, effs := p.HandleCast(c, sock)
	if want := gen.Do(statsRef.CastEffect(connOpened{}), sock.SetActiveEffect(gentcp.Once)); !c.open || !reflect.DeepEqual(effs, want) {
		t.Errorf("on the socket: %+v, %#v", c, effs)
	}
	for _, b := range []string{"hi", "there"} {
		c, effs = p.HandleInfo(c, gentcp.DataMsg{Sock: sock, Bytes: []byte(b)})
		want := gen.Do(sock.SendActiveEffect([]byte(b), gentcp.Once))
		if !reflect.DeepEqual(effs, want) {
			t.Errorf("on data: %#v", effs)
		}
	}
	if _, effs = p.HandleInfo(c, gentcp.ClosedMsg{Sock: sock}); !reflect.DeepEqual(effs, gen.Do(gen.Stop{})) {
		t.Errorf("on closed: %#v", effs)
	}
	want := gen.Do(statsRef.CastEffect(echoed{7}), statsRef.CastEffect(connClosed{}))
	if effs = p.Terminate(c, nil); !reflect.DeepEqual(effs, want) {
		t.Errorf("on terminate: %#v", effs)
	}
	if effs = p.Terminate(conn{}, nil); effs != nil {
		t.Errorf("terminate before the socket: %#v", effs)
	}
}
