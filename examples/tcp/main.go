// Command tcp is an echo server built as an OTP-style application:
//
//	echo_sup (rest_for_one)
//	├── echo_stats     gen_server counting connections and bytes
//	├── echo_conns     dynamic supervisor; one echo_protocol per connection
//	└── echo_listener  the listening socket, and the acceptors linked to it
//
// Each file holds one "module": main.go is the application, echo_sup.go
// the supervisor, echo_stats.go a gen_server, echo_listener.go and
// echo_acceptor.go plain processes using gentcp as gen_tcp, and
// echo_protocol.go the gen_server of a connection, which owns its socket.
//
//	go run ./examples/tcp -addr 127.0.0.1:5555
//	nc 127.0.0.1 5555
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/shun159/molecule/proc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5555", "address to listen on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	n := proc.NewNode("echo@localhost")
	app, a, err := startEcho(context.Background(), n, *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("echo: listening on %v", a)

	select {
	case <-ctx.Done():
	case <-app.Done(): // the tree gave up
	}
	if s, err := statsRef.Call(context.Background(), n, GetStats{}); err == nil {
		log.Printf("echo: served %d connections, %d bytes", s.Total, s.Bytes)
	}
	// The tree stops in order: the listener, the connections, the stats.
	if err := app.Stop(context.Background()); err != nil {
		log.Fatal(err)
	}
	if err := app.Err(); err != nil {
		log.Fatal(err)
	}
}
