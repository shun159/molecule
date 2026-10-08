package main

import (
	"context"
	"fmt"
	"io"

	"github.com/shun159/molecule/proc"
)

const consoleName = "console"

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
