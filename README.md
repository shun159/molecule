# molecule

[![CI](https://github.com/shun159/molecule/actions/workflows/ci.yml/badge.svg)](https://github.com/shun159/molecule/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/shun159/molecule.svg)](https://pkg.go.dev/github.com/shun159/molecule)

molecule is a library for writing Go programs the way Erlang/OTP programs
are written: processes with mailboxes, links and monitors, behaviours, and
supervisors.

## Goals

molecule aims to let a program be built from many small processes that fail
on their own. A process that panics exits with a reason, as an Erlang process
would; the processes linked to it and the supervisor above it decide what
happens next. Supervisors restart children with the strategies of OTP
(one_for_one, one_for_all, rest_for_one, and dynamic children), within a
restart intensity.

Behaviours are pure. A callback gets the state and a message and returns the
next state along with effects: replies, sends, timers, monitors. The runtime
performs the effects. A behaviour is therefore tested by calling its
functions, without starting a process.

## Example

A counter, as a gen_server. Its state is the count; a cast adds to it, a
call returns it.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/behaviours/genserver"
	"github.com/shun159/molecule/proc"
)

// Counter counts; its state is the count.
type Counter struct{ genserver.Default[int] }

type Get struct{}
type Add struct{ N int }

func (Counter) HandleCall(n int, _ Get, from genserver.From[int]) (int, []molecule.Effect) {
	return n, molecule.Do(from.Reply(n))
}

func (Counter) HandleCast(n int, a Add) (int, []molecule.Effect) {
	return n + a.N, nil
}

func main() {
	ctx := context.Background()
	node := proc.NewNode("")
	counter, err := genserver.Start(ctx, node, Counter{})
	if err != nil {
		log.Fatal(err)
	}
	counter.Cast(node, Add{2})
	counter.Cast(node, Add{3})
	n, err := counter.Call(ctx, node, Get{})
	fmt.Println(n, err) // 5 <nil>
}
```

`Default` supplies the callbacks the counter does not need, Init among
them: the count starts at zero. The reply is an effect returned by
HandleCall, not a call made from it, so the callbacks are tested without a
process:

```go
n, effs := Counter{}.HandleCast(2, Add{3}) // 5, no effects
```

## Limits

- Messages are not copied. A pointer, slice or map sent to another
  process is shared with it; send values, or do not touch what was sent.
  Between nodes, messages are encoded and so copied.
- Nothing stops a process from sharing memory through other means, such
  as goroutines, channels or mutexes of its own.
- A process cannot be stopped from outside. Killed, it is dead at once to
  the others, but its goroutine runs until it next receives; a process in
  a loop that never receives runs on.
- A panic in a goroutine that is not a process ends the whole program.
- Processes share one heap; there is no limit on the memory of one.

## Packages

    molecule                   names, calls, casts and effects: the vocabulary of programs
    proc                       processes, links, monitors, names, synchronous start
    behaviours/genserver       gen_server
    behaviours/genstatem       gen_statem
    behaviours/supervisor      supervisors, static and dynamic
    behaviours/gen             the runtime of behaviours, for those building them
    application                applications: trees started and stopped in order
    net/gentcp                 TCP sockets owned by processes, like gen_tcp
    net/gentcpacceptor         TCP acceptor pool and connection behaviour
    net/genudp                 UDP sockets owned by processes, like gen_udp
    dist                       distribution: processes of several nodes, by PID
    dist/pg                    process groups, shared by the nodes
    gensim                     deterministic simulation of behaviours
    observer                   processes and supervision trees of a running node, over HTTP

## Documentation

The package documentation is available through `go doc`. The `examples`
directory holds small programs: an echo server, a chat server, a server
crashing and being restarted, a mailbox under load, the code lock and
push button of the gen_statem documentation, two nodes pinging each
other, and a standby node taking over when its primary goes away.

molecule requires Go 1.27.
