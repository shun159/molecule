package dgram

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
	// Passive leaves datagrams in the socket until Recv takes them, like
	// {active, false}. It is the default.
	Passive = Active{}
	// Once sends the next datagram as a DataMsg, then turns passive, like
	// {active, once}.
	Once = Active{kind: once}
	// Always sends every datagram as a DataMsg, like {active, true}. A slow
	// owner sees its mailbox grow.
	Always = Active{kind: always}
)

// N sends the next n datagrams as DataMsg messages, then a PassiveMsg,
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

// take accounts for one datagram sent as a DataMsg, and reports whether
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
