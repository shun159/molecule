package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

// pinger pings the pong server of node every interval, and prints what
// comes back: a pong, or why not. When the other node goes away, the
// calls fail with noconnection; when it comes back, the next call
// connects again.
func pinger(w io.Writer, node string, interval time.Duration) func(*proc.Self) error {
	return func(self *proc.Self) error {
		pong := genserver.RefFor(PongServer{}, gen.Remote{Node: node, Name: string(pongName)})
		for i := 1; ; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), interval)
			rep, err := pong.Call(ctx, self, fmt.Sprintf("ping #%d from %s", i, self.Node().Name()))
			cancel()
			if err != nil {
				fmt.Fprintf(w, "ping #%d: %v\n", i, err)
			} else {
				fmt.Fprintln(w, rep)
			}
			wait, cancel := context.WithTimeout(context.Background(), interval)
			_, err = self.Receive(wait) // nothing comes: it is the pause
			cancel()
			if err != nil && wait.Err() == nil {
				return err // the process was stopped
			}
		}
	}
}
