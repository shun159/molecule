package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/pg"
	"github.com/shun159/molecule/proc"
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

// The protocol is pure: it is tested as calls, a line at a time, as the
// socket cuts them.
func TestLines(t *testing.T) {
	p := ChatProtocol{}
	self := proc.NewNode("").NewPID()
	sock := gentcp.Socket{}
	c, _, _ := p.Init(self, sock)

	c, effs := p.HandleData(c, sock, []byte("\r\n"))
	if effs != nil || c.nick != "" {
		t.Fatalf("empty line: %+v %#v", c, effs)
	}
	c, effs = p.HandleData(c, sock, []byte("alice\r\n"))
	want := molecule.Do(
		pg.JoinEffect(scope, lobby, self),
		pg.SendEffect(scope, lobby, said{Text: "alice joined"}, proc.PID{}),
	)
	if c.nick != "alice" || !reflect.DeepEqual(effs, want) {
		t.Fatalf("nick: %+v %#v", c, effs)
	}
	_, effs = p.HandleData(c, sock, []byte("hello\n"))
	want = molecule.Do(pg.SendEffect(scope, lobby, said{From: "alice", Text: "hello"}, proc.PID{}))
	if !reflect.DeepEqual(effs, want) {
		t.Errorf("line: %#v", effs)
	}
}
