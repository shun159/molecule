package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/shun159/molecule/dist/pg"
	"github.com/shun159/molecule/proc"
)

// console reads commands from in and runs them from n, writing what comes
// of them, and what is published on the topics subscribed, to out.
type console struct {
	n       *proc.Node
	out     io.Writer
	printer proc.PID            // writes what the subscribers get
	subs    map[string]proc.PID // the subscriber of each topic, on this node
}

func newConsole(n *proc.Node, out io.Writer) *console {
	printer := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			fmt.Fprintln(out, msg)
		}
	})
	return &console{n: n, out: out, printer: printer, subs: map[string]proc.PID{}}
}

func (c *console) run(in io.Reader) {
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		switch f := strings.Fields(sc.Text()); {
		case len(f) == 0:
		case len(f) == 2 && f[0] == "sub":
			c.subscribe(f[1])
		case len(f) == 2 && f[0] == "unsub":
			c.unsubscribe(f[1])
		case len(f) >= 3 && f[0] == "pub":
			Publish(c.n, f[1], strings.Join(f[2:], " "))
		case len(f) == 1 && f[0] == "topics":
			c.topics()
		default:
			fmt.Fprintln(c.out, "sub TOPIC, unsub TOPIC, pub TOPIC TEXT, or topics")
		}
	}
}

func (c *console) subscribe(topic string) {
	if _, ok := c.subs[topic]; ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sub, err := Subscribe(ctx, c.n, topic, c.printer)
	if err != nil {
		fmt.Fprintln(c.out, "error:", err)
		return
	}
	c.subs[topic] = sub
}

func (c *console) unsubscribe(topic string) {
	sub, ok := c.subs[topic]
	if !ok {
		return
	}
	delete(c.subs, topic)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := Unsubscribe(ctx, c.n, sub); err != nil {
		fmt.Fprintln(c.out, "error:", err)
	}
}

// topics writes the topics subscribed to, on any node, with how many
// subscribe to each.
func (c *console) topics() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	groups, err := pg.Which(ctx, c.n, scope)
	if err != nil {
		fmt.Fprintln(c.out, "error:", err)
		return
	}
	for _, g := range groups {
		members, _ := pg.Members(ctx, c.n, scope, g)
		fmt.Fprintf(c.out, "%v: %d\n", g, len(members))
	}
}
