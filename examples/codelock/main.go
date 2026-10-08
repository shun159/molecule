// Command codelock is the code lock of the gen_statem documentation: a
// door that unlocks on the right code, and locks again after a while.
//
//	code_lock  gen_statem, states Locked and Open
//	display    process printing what the lock tells it
//
// Each file holds one "module": main.go plays a scenario, code_lock.go is
// the state machine, display.go the process standing for the door.
//
//	go run ./examples/codelock
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shun159/molecule/behaviours/genstatem"
	"github.com/shun159/molecule/proc"
)

func main() {
	if err := run(os.Stdout, 2*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run presses buttons on a lock with code 1-2-3, which stays open for
// openTime, and writes what the door shows to w.
func run(w io.Writer, openTime time.Duration) error {
	ctx := context.Background()
	n := proc.NewNode("codelock@localhost")
	display := startDisplay(n, w)
	pid, err := genstatem.Start(ctx, n, CodeLock{Code: []int{1, 2, 3}, OpenTime: openTime, Display: display})
	if err != nil {
		return err
	}
	lock := genstatem.NewRef(pid)
	press := func(buttons ...int) {
		for _, b := range buttons {
			lock.Cast(n, Button{b})
		}
	}
	status := func() {
		v, _ := lock.Call(ctx, n, Status{})
		show(n, display, fmt.Sprintf("status: %v", v))
	}

	press(1, 2, 4) // wrong
	press(1, 2, 3) // right: opens
	press(4, 5, 6) // while open: postponed until locked again
	status()
	time.Sleep(openTime + openTime/2) // locks again, then takes 4, 5, 6
	status()
	press(1) // and then nothing for a while: cleared
	time.Sleep(clearTime + time.Second)
	press(1, 2, 3) // so the code works from the start again

	// Wait for the lock to have handled all, so that what it sent the
	// display is in before the sync.
	lock.Call(ctx, n, Status{})
	sync(n, display)
	shutdown(n, pid, display)
	return nil
}

// shutdown kills the processes and waits until they are gone.
func shutdown(n *proc.Node, pids ...proc.PID) {
	for _, pid := range pids {
		down, stop := n.Watch(context.Background(), pid)
		n.Spawn(func(s *proc.Self) error { s.Exit(pid, proc.Kill); return nil })
		<-down.Done()
		stop()
	}
}
