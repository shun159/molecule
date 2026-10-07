// Command pushbutton runs the pushbutton gen_statem of the OTP
// documentation: each push toggles it between off and on, and it counts
// how often it went on.
//
//	go run ./examples/pushbutton
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/shun159/molecule/proc"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(w io.Writer) error {
	ctx := context.Background()
	n := proc.NewNode("pushbutton@localhost")
	if _, err := start(ctx, n); err != nil {
		return err
	}
	for range 3 {
		status, err := push(ctx, n)
		if err != nil {
			return err
		}
		fmt.Fprintln(w, "push:", status)
	}
	count, err := getCount(ctx, n)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "count:", count)
	return stop(ctx, n)
}
