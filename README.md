# molecule

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
    dist                       distribution: processes of several nodes, by PID
    dist/pg                    process groups, shared by the nodes
    gensim                     deterministic simulation of behaviours

## Documentation

The package documentation is available through `go doc`. The `examples`
directory holds small programs: an echo server, a chat server, a server
crashing and being restarted, a mailbox under load, the code lock and
push button of the gen_statem documentation, two nodes pinging each
other, and a standby node taking over when its primary goes away.

molecule requires Go 1.27.
