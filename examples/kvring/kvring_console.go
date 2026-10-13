package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/shun159/molecule/proc"
)

// console reads commands from in, put and get, and runs them from n,
// writing what comes of them to out.
type console struct {
	n    *proc.Node
	ring Ring
	out  io.Writer
}

func (c console) run(in io.Reader) {
	sc := bufio.NewScanner(in)
	for sc.Scan() {
		switch f := strings.Fields(sc.Text()); {
		case len(f) == 0:
		case len(f) == 3 && f[0] == "put":
			c.put(f[1], f[2])
		case len(f) == 2 && f[0] == "get":
			c.get(f[1])
		default:
			fmt.Fprintln(c.out, "put KEY VALUE, or get KEY")
		}
	}
}

func (c console) put(key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := Put(ctx, c.n, c.ring, key, value)
	if err != nil {
		c.failed(key, err)
		return
	}
	fmt.Fprintf(c.out, "ok (%s)\n", r.Node)
}

func (c console) get(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := Get(ctx, c.n, c.ring, key)
	switch {
	case err != nil:
		c.failed(key, err)
	case r.Found:
		fmt.Fprintf(c.out, "%s (%s)\n", r.Value, r.Node)
	default:
		fmt.Fprintf(c.out, "not found (%s)\n", r.Node)
	}
}

func (c console) failed(key string, err error) {
	fmt.Fprintf(c.out, "error: %v (%s)\n", err, c.ring.Owner(key))
}
