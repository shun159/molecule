package proc

// Alias creates a one-shot address, like erlang:alias([reply]): the first
// message sent to ref with SendAlias is delivered on the returned channel
// and the alias is deactivated. Unlike a PID it is not tied to a process,
// so code outside processes can receive through it, and like a PID it is
// plain data, so it can be handed to a remote node.
//
// unalias deactivates the alias if nothing has arrived yet. It must be
// called once the alias is no longer needed, to release it.
func (n *Node) Alias() (ref Ref, msgs <-chan any, unalias func()) {
	ref = n.MakeRef()
	ch := make(chan any, 1)
	n.aliases.Store(ref.id, ch)
	return ref, ch, func() { n.aliases.Delete(ref.id) }
}

// SendAlias delivers msg to the alias ref. Like Send it never fails:
// messages to inactive, stale or unreachable aliases are dropped.
func (n *Node) SendAlias(ref Ref, msg any) {
	if ref.node != n.name || ref.creation != n.creation {
		return // TODO(dist): route to remote aliases.
	}
	if ch, ok := n.aliases.LoadAndDelete(ref.id); ok {
		ch.(chan any) <- msg // never blocks: only the first message gets here
	}
}
