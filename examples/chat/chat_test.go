package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcpacceptor"
	"github.com/shun159/molecule/pg"
	"github.com/shun159/molecule/proc"
	"github.com/shun159/molecule/supervisor"
)

type client struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

// join connects and gives a nick, after the prompt.
func join(t *testing.T, addr, nick string) *client {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	c := &client{t: t, conn: conn, r: bufio.NewReader(conn)}
	prompt := make([]byte, len("nick? "))
	if _, err := io.ReadFull(c.r, prompt); err != nil || string(prompt) != "nick? " {
		t.Fatalf("prompt %q, %v", prompt, err)
	}
	c.say(nick)
	return c
}

func (c *client) say(line string) {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, line+"\n"); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) expect(want string) {
	c.t.Helper()
	got, err := c.r.ReadString('\n')
	if err != nil || got != want+"\n" {
		c.t.Fatalf("read %q, %v; want %q", got, err, want)
	}
}

func TestChat(t *testing.T) {
	n := proc.NewNode("")
	ctx := context.Background()
	app, err := startChat(ctx, n, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer supervisor.Stop(ctx, n, app.sup)
	addr := app.listener.Addr().String()

	alice := join(t, addr, "alice")
	alice.expect("* alice joined")
	bob := join(t, addr, "bob")
	bob.expect("* bob joined")
	alice.expect("* bob joined")

	alice.say("hi bob")
	alice.expect("<alice> hi bob")
	bob.expect("<alice> hi bob")

	bob.conn.Close()
	alice.expect("* bob left")

	// The scope took bob out of the lobby.
	deadline := time.Now().Add(5 * time.Second)
	for {
		members, err := pg.Members(ctx, n, scope, lobby)
		if err == nil && len(members) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lobby = %v, %v", members, err)
		}
		time.Sleep(time.Millisecond)
	}
}

// The protocol is pure: lines arriving in pieces are tested as calls.
func TestLinesInPieces(t *testing.T) {
	p := ChatProtocol{}
	self := proc.NewNode("").NewPID()
	sock := gentcpacceptor.Socket{}
	c, _, _ := p.Init(self, sock)

	c, effs := p.HandleData(c, sock, []byte("ali"))
	if effs != nil || c.nick != "" {
		t.Fatalf("half a nick: %+v %#v", c, effs)
	}
	c, effs = p.HandleData(c, sock, []byte("ce\r\nhel"))
	want := gen.Do(
		pg.JoinEffect(scope, lobby, self),
		pg.SendEffect(scope, lobby, said{Text: "alice joined"}, proc.PID{}),
	)
	if c.nick != "alice" || !reflect.DeepEqual(effs, want) {
		t.Fatalf("nick: %+v %#v", c, effs)
	}
	c, effs = p.HandleData(c, sock, []byte("lo\n\nbye\n"))
	want = gen.Do(
		pg.SendEffect(scope, lobby, said{From: "alice", Text: "hello"}, proc.PID{}),
		pg.SendEffect(scope, lobby, said{From: "alice", Text: "bye"}, proc.PID{}),
	)
	if !reflect.DeepEqual(effs, want) || len(c.buf) != 0 {
		t.Errorf("lines: %+v %#v", c, effs)
	}
}
