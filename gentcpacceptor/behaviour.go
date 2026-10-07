package gentcpacceptor

import (
	"fmt"
	"slices"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// Behaviour handles one connection with state S.
//
// The runtime reads ahead of the handler, but only so far: at most
// Spec.ActiveN reads are delivered and waiting to be handled, so a slow
// handler is never flooded. Each HandleData allows one more.
//
// A Behaviour may also implement ClosedHandler, InfoHandler and
// Terminator.
type Behaviour[S any] interface {
	// Init is called when the connection is ready. self is the PID of the
	// connection process. An error stops it.
	Init(self proc.PID, sock Socket) (S, []gen.Effect, error)
	// HandleData handles bytes read from the connection, as much as one
	// read returned: a message may come in pieces, or several at once.
	HandleData(state S, sock Socket, data []byte) (S, []gen.Effect)
}

// ClosedHandler is called when the connection is closed, by the peer
// (err is nil) or by an error. Without it, the handler stops: normally
// when the peer closed, and otherwise with err wrapped in proc.Shutdown,
// as a connection lost is no failure of the handler to report.
type ClosedHandler[S any] interface {
	HandleClosed(state S, sock Socket, err error) (S, []gen.Effect)
}

// InfoHandler handles any other message sent to the connection process,
// gen.CallMsg, gen.CastMsg and gen.ContinueMsg included. Without it, they
// are dropped.
type InfoHandler[S any] interface {
	HandleInfo(state S, sock Socket, msg any) (S, []gen.Effect)
}

// Terminator is called when the connection process stops, see
// gen.Behaviour. The connection is closed right after.
type Terminator[S any] interface {
	Terminate(state S, reason error) []gen.Effect
}

// conn is the state of the connection process: the state of the
// behaviour, once the socket is attached.
//
// The runtime copies its state from call to call, so conn stays small:
// what does not change for the life of the connection is behind a pointer.
// What it points to is never modified, so sharing it keeps the state pure.
type conn[S any] struct {
	id    *ident
	ready bool
	state S
}

// ident is what a connection process knows of itself.
type ident struct {
	self proc.PID
	sock Socket
}

// adapter runs a Behaviour as a gen.Behaviour.
type adapter[S any] struct {
	b       Behaviour[S]
	activeN int
}

func (adapter[S]) Init(self proc.PID, _ any) (conn[S], []gen.Effect, error) {
	return conn[S]{id: &ident{self: self}}, nil, nil
}

func (a adapter[S]) Handle(c conn[S], msg gen.Msg) (conn[S], []gen.Effect) {
	info, ok := msg.(gen.InfoMsg)
	if !ok {
		return a.info(c, msg) // a CallMsg or a CastMsg
	}
	switch m := info.Msg.(type) {
	case attached:
		state, effs, err := a.b.Init(c.id.self, m.sock)
		if err != nil {
			return c, gen.Do(gen.Stop{Reason: err})
		}
		id := &ident{self: c.id.self, sock: m.sock}
		return conn[S]{id: id, ready: true, state: state}, then(effs, m.sock.active(a.activeN))
	case data:
		if !c.ready || m.sock != c.id.sock.PID {
			return c, nil
		}
		var effs []gen.Effect
		c.state, effs = a.b.HandleData(c.state, c.id.sock, m.b)
		return c, then(effs, c.id.sock.active(1))
	case closed:
		if !c.ready || m.sock != c.id.sock.PID {
			return c, nil
		}
		if h, ok := a.b.(ClosedHandler[S]); ok {
			var effs []gen.Effect
			c.state, effs = h.HandleClosed(c.state, c.id.sock, m.err)
			return c, effs
		}
		if m.err == nil {
			return c, gen.Do(gen.Stop{})
		}
		return c, gen.Do(gen.Stop{Reason: fmt.Errorf("%w: %w", proc.Shutdown, m.err)})
	}
	return a.info(c, info.Msg)
}

func (a adapter[S]) info(c conn[S], msg any) (conn[S], []gen.Effect) {
	h, ok := a.b.(InfoHandler[S])
	if !ok || !c.ready {
		return c, nil
	}
	var effs []gen.Effect
	c.state, effs = h.HandleInfo(c.state, c.id.sock, msg)
	return c, effs
}

func (a adapter[S]) Terminate(c conn[S], reason error) []gen.Effect {
	if t, ok := a.b.(Terminator[S]); ok && c.ready {
		return t.Terminate(c.state, reason)
	}
	return nil
}

// then appends e to effs, which belong to the behaviour and are not
// modified.
func then(effs []gen.Effect, e gen.Effect) []gen.Effect {
	return append(slices.Clip(effs), e)
}
