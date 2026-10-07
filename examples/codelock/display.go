package main

import (
	"context"
	"fmt"
	"io"

	"github.com/shun159/molecule/proc"
)

// startDisplay starts the process standing for the door: it prints the
// lines it is sent to w.
func startDisplay(n *proc.Node, w io.Writer) proc.PID {
	return n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			switch m := msg.(type) {
			case string:
				fmt.Fprintln(w, m)
			case syncReq:
				close(m.done)
			}
		}
	})
}

// show prints text on the display, in order with what the lock sent.
func show(n *proc.Node, display proc.PID, text string) { n.Send(display, text) }

type syncReq struct{ done chan struct{} }

// sync returns once the display has printed what it was sent before.
func sync(n *proc.Node, display proc.PID) {
	done := make(chan struct{})
	n.Send(display, syncReq{done})
	<-done
}
