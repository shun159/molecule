package main

import (
	"context"
	"fmt"
	"io"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

const consoleName = "console"

// consoleChild starts the console, registered, under a supervisor.
func consoleChild(w io.Writer) supervisor.Starter {
	return supervisor.StartFunc(func(ctx context.Context, parent *proc.Self) (proc.PID, error) {
		return parent.StartLink(ctx, func(self *proc.Self) error {
			if err := self.Node().Register(consoleName, self.PID()); err != nil {
				self.InitAck(err)
				return err
			}
			self.InitAck(nil)
			return console(w)(self)
		})
	})
}

// console prints the lines sent to it, so that the workers print without
// leaving their pure functions.
func console(w io.Writer) func(*proc.Self) error {
	return func(self *proc.Self) error {
		for {
			msg, err := self.Receive(context.Background())
			if err != nil {
				return err
			}
			fmt.Fprintln(w, msg)
		}
	}
}
