// Command crash shows "let it crash": a gen_server that panics is
// restarted by its supervisor, under the same name and with a fresh state,
// until it crashes too often and the supervisor gives up.
//
//	crash_sup (one_for_one, at most 3 restarts in 5s)
//	└── calc  gen_server holding a number; Div{0} panics
//
// Each file holds one "module": main.go is the application and the client,
// crash_sup.go the supervisor, calc_server.go the gen_server.
//
//	go run ./examples/crash
//
// The client's view goes to stdout, the reports to stderr.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shun159/molecule/proc"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:       slog.LevelDebug, // show the restarts too
		ReplaceAttr: shortStack,
	}))
	if err := run(os.Stdout, logger); err != nil {
		log.Fatal(err)
	}
}

// run plays the scenario, writing what the client sees to w.
func run(w io.Writer, logger *slog.Logger) error {
	ctx := context.Background()
	n := proc.NewNode("crash@localhost", proc.WithLogger(logger))
	sup, err := startCrashSup(ctx, n)
	if err != nil {
		return err
	}

	calc := calcRef
	calc.Cast(n, Add{40})
	calc.Cast(n, Add{2})
	v, err := calc.Call(ctx, n, Get{})
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "value: %d\n", v)

	for i := 1; i <= 3; i++ {
		// Dividing by zero panics in the server: the call fails with the
		// server's exit reason, and the supervisor restarts it.
		_, err = calc.Call(ctx, n, Div{By: 0})
		fmt.Fprintf(w, "crash %d: call failed: %v\n", i, err)

		v, err = callAfterRestart(ctx, n, Get{})
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "crash %d: restarted, value back to %d\n", i, v)
		calc.Cast(n, Add{i})
	}

	// One more right away is too many restarts within the period: the
	// supervisor stops, and with it the server.
	down, stop := n.Watch(ctx, sup)
	defer stop()
	calc.Call(ctx, n, Div{By: 0})
	<-down.Done()
	fmt.Fprintf(w, "crash 4: supervisor gave up: %v\n", context.Cause(down))
	_, err = calc.Call(ctx, n, Get{})
	fmt.Fprintf(w, "calc is gone: %v\n", err)
	return nil
}

// callAfterRestart calls the server, waiting for its restart: until the
// supervisor has started the new one, the name is not registered.
func callAfterRestart(ctx context.Context, n *proc.Node, req CalcReq) (int, error) {
	deadline := time.Now().Add(time.Second)
	for {
		v, err := calcRef.Call(ctx, n, req)
		if !errors.Is(err, proc.NoProc) || time.Now().After(deadline) {
			return v, err
		}
		time.Sleep(time.Millisecond)
	}
}

// shortStack keeps, of a panic's stack, the frames of this program, which
// show where it panicked: "function at file:line", innermost first.
func shortStack(_ []string, a slog.Attr) slog.Attr {
	if a.Key != "stack" {
		return a
	}
	var keep []string
	var function string
	for line := range strings.Lines(a.Value.String()) {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "examples/crash/") {
			function = line // a frame is a function line, then its file line
			continue
		}
		file, _, _ := strings.Cut(filepath.Base(line), " ")
		keep = append(keep, function+" at "+file)
	}
	return slog.String("stack", strings.Join(keep, " <- "))
}
