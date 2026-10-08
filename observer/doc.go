// Package observer shows what a node runs, as Erlang's observer does from
// a shell: its processes, those with the most messages waiting first, and
// the supervision trees of its applications, with the restarts of their
// children.
//
// [Processes] and [TreeOf] return them as values, [WriteProcesses] and
// [WriteTree] write them as text, the many connections of a pool as one
// line, and [Handler] serves them over HTTP,
// for a look from outside a running program:
//
//	go http.ListenAndServe("127.0.0.1:8080", observer.Handler(n, apps))
//
//	$ curl localhost:8080/tree
//	application echo
//	<echo@localhost.12.1>  supervisor
//	├─ echo_stats  <echo@localhost.13.1>  genserver main.EchoStats
//	├─ echo_conns  <echo@localhost.14.1>  dynamic supervisor
//	│  └─ <echo@localhost.31.1>  genserver main.EchoProtocol
//	└─ echo_listener  <echo@localhost.15.1>
//
// The labels are what the processes are, as their runtimes tell it, see
// proc.Self.SetLabel.
package observer
