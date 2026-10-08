// Package dist connects nodes, as the distribution of Erlang does: once a
// node is distributed, its processes send to, link to and monitor the
// processes of other nodes as they do their own, by PID.
//
//	n := proc.NewNode("a@host")
//	d, err := dist.Start(n, dist.Config{
//		Listen:  ":4370",
//		Cookie:  "secret",
//		Resolve: func(node string) (string, bool) { return addrs[node] },
//	})
//
// # Connections
//
// A node connects to another the first time one of its processes sends
// to it, links to or monitors one of its processes, or with
// [Dist.Connect]. A connection is a gentcp socket owned by a process of
// its own, one per node. Nodes find each other by name with
// Config.Resolve, and prove they share the cookie when connecting: each
// shows it has it without sending it. Two nodes connecting to each other
// at once end with one connection.
//
// With Config.TLS, connections are secured by TLS, and nodes may prove
// themselves by certificates, as with inet_tls_dist.
//
// What a process sends to another arrives in order, messages and exit
// signals together. When a connection is lost, or cannot be made, the
// links through it fail with proc.NoConnection, as do the monitors, and
// what was on its way is lost. Sending connects again.
//
// # Messages
//
// Messages are Go values, of the types given to [Register], sent with
// their exported fields. PIDs and Refs keep their identity across nodes.
// A message that cannot be encoded is dropped and logged, as one that
// cannot be decoded. Config.Codec replaces the encoding.
//
// The registered names of other nodes are reached with proc.Node.SendName
// and molecule.Remote, by which gen calls and casts a server of another node:
//
//	ref := genserver.NewRef[Req, Rep, Cast](molecule.Remote{Node: "b@host", Name: "server"})
//	rep, err := ref.Call(ctx, self, req)
//
// Exit reasons are the reasons of proc, or a [RemoteError] carrying the
// text of another: errors.Is(reason, proc.Shutdown) holds across nodes for
// a reason wrapping it.
//
// A connection silent for Config.TickTime is taken for lost, like
// net_ticktime: an idle connection sends ticks to show it lives.
//
// # What there is not yet
//
// A node restarted under the same name is refused until the connection to
// its previous incarnation is noticed lost.
package dist
