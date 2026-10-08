// Command chat is a chat server: each connection is a process, and the
// people in the lobby are a process group.
//
//	chat_sup (rest_for_one)
//	├── chat_pg        pg scope holding the lobby
//	└── chat_listener  gen_tcp_acceptor; one chat_protocol per connection
//
// Each file holds one "module": main.go is the application, chat_sup.go
// the supervisor, chat_protocol.go the connection behaviour.
//
//	go run ./examples/chat -addr 127.0.0.1:5555
//	nc 127.0.0.1 5555
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/shun159/molecule/behaviours/supervisor"
	"github.com/shun159/molecule/proc"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5555", "address to listen on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n := proc.NewNode("chat@localhost")
	app, err := startChat(context.Background(), n, *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("chat: listening on %v", app.listener.Addr())
	<-ctx.Done()
	if err := supervisor.Stop(context.Background(), n, app.sup); err != nil {
		log.Fatal(err)
	}
}
