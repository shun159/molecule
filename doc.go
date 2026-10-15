// Package molecule is a library for writing Go programs the way Erlang/OTP
// programs are written: processes with mailboxes, links and monitors,
// behaviours, and supervisors. This package holds no code; the library is
// in its packages:
//
//	actor                  names, calls, casts and effects: the vocabulary of programs
//	proc                   processes, links, monitors, names, synchronous start
//	behaviours/genserver   gen_server
//	behaviours/genstatem   gen_statem
//	behaviours/supervisor  supervisors, static and dynamic
//	behaviours/gen         the runtime of behaviours, for those building them
//	application            applications: trees started and stopped in order
//	net/gentcp             TCP sockets owned by processes, like gen_tcp
//	net/gentcpacceptor     TCP acceptor pool and connection behaviour
//	net/genudp             UDP sockets owned by processes, like gen_udp
//	net/socket             any socket(2) owned by a process
//	kvs                    a table of keys and values in memory, kept in a log on disk
//	pmap                   a map never changed, for pure states, as Erlang's maps
//	dist                   distribution: processes of several nodes, by PID
//	dist/pg                process groups, shared by the nodes
//	gensim                 deterministic simulation of behaviours
//	observer               processes and supervision trees of a running node, over HTTP
package molecule
