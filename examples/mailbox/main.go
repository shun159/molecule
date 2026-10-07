// Command mailbox floods one mailbox: N mailer processes each send M
// messages to a single receiver, which counts them. It is the Pony example
// of the same name, with processes for actors and casts for behaviours.
//
//	main ── spawns N ──> mailer ── M × Pong ──> receiver (gen_server)
//
// Each file holds one "module": main.go starts it all, receiver.go is the
// gen_server counting, mailer.go the process sending.
//
//	go run ./examples/mailbox 3 1000000
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/shun159/molecule/genserver"
	"github.com/shun159/molecule/proc"
)

const usage = `mailbox OPTIONS
  N   number of sending actors
  M   number of messages to pass from each sender to the receiver
`

var errUsage = errors.New("usage")

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, errUsage) {
			fmt.Fprint(os.Stderr, usage)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run starts a receiver expecting N×M messages, N mailers sending M each,
// and waits until the receiver has them all.
func run(args []string, w io.Writer) error {
	size, pass, err := parseArgs(args)
	if err != nil {
		return err
	}

	ctx := context.Background()
	n := proc.NewNode("mailbox@localhost")
	receiver, err := genserver.Start(ctx, n, Receiver{Expect: size * pass})
	if err != nil {
		return err
	}

	start := time.Now()
	for range size {
		ping(n, receiver, pass)
	}
	pongs, err := receiver.Call(ctx, n, Wait{})
	if err != nil {
		return err
	}
	elapsed := time.Since(start)

	fmt.Fprintf(w, "received %d pongs from %d mailers × %d messages\n", pongs, size, pass)
	fmt.Fprintf(w, "in %v (%.0f messages/s)\n", elapsed.Round(time.Microsecond), float64(pongs)/elapsed.Seconds())
	return nil
}

func parseArgs(args []string) (size, pass int, err error) {
	if len(args) != 2 {
		return 0, 0, errUsage
	}
	size, err1 := strconv.Atoi(args[0])
	pass, err2 := strconv.Atoi(args[1])
	if err1 != nil || err2 != nil || size < 0 || pass < 0 {
		return 0, 0, errUsage
	}
	return size, pass, nil
}
