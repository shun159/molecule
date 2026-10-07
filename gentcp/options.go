package gentcp

import "time"

// Options configure a socket, like the options of gen_tcp. The zero value
// is a passive socket without framing that closes when the peer does.
type Options struct {
	// Active is how data reaches the owner: taken with Recv, or sent as
	// DataMsg messages.
	Active Active
	// Packet is how the bytes are cut into packets.
	Packet Packet
	// PacketSize, if positive, bounds the size of a packet received.
	PacketSize int
	// HalfClosed keeps the socket open for sending once the peer has
	// closed its side, like {exit_on_close, false}. By default the socket
	// closes then.
	HalfClosed bool
	// SendTimeout, if positive, bounds how long Send waits for the peer
	// to take the data.
	SendTimeout time.Duration
}

// Active is the active mode of a socket, like the active option.
type Active struct {
	kind activeKind
	n    int
}

type activeKind int

const (
	passive activeKind = iota
	once
	count
	always
)

var (
	// Passive leaves the data in the socket until Recv takes it, like
	// {active, false}. It is the default: the peer is held back while the
	// owner does not read.
	Passive = Active{}
	// Once sends the next packet as a DataMsg, then turns passive,
	// like {active, once}.
	Once = Active{kind: once}
	// Always sends every packet as a DataMsg, like {active, true}.
	// Nothing holds the peer back: a slow owner sees its mailbox grow.
	Always = Active{kind: always}
)

// N sends the next n packets as DataMsg messages, then a PassiveMsg,
// turning passive, like {active, N}. Setting N on a socket already in this
// mode adds n to what is left; a total of zero or less turns it passive at
// once.
func N(n int) Active { return Active{kind: count, n: n} }

// set applies a new active mode to a, and reports whether the socket has
// turned passive through a count, which tells the owner with PassiveMsg.
func (a Active) set(to Active) (Active, bool) {
	if to.kind != count {
		return to, false
	}
	if a.kind == count {
		to.n += a.n
	}
	if to.n <= 0 {
		return Passive, true
	}
	return to, false
}

// take accounts for one packet sent as a DataMsg, and reports whether
// the socket has turned passive through a count.
func (a Active) take() (Active, bool) {
	switch a.kind {
	case once:
		return Passive, false
	case count:
		if a.n--; a.n <= 0 {
			return Passive, true
		}
	}
	return a, false
}
