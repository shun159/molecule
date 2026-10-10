package genudp

import "github.com/shun159/molecule/net/internal/dgram"

// maxPacketSize is the largest UDP payload, the default bound on a
// datagram received.
const maxPacketSize = 65535

// Options configure a socket, like the options of gen_udp. The zero value
// is a passive socket receiving datagrams of any size.
type Options struct {
	// Active is how datagrams reach the owner: taken with Recv, or sent as
	// DataMsg messages.
	Active Active
	// PacketSize, if positive, bounds the size of a datagram received; a
	// larger one is dropped.
	PacketSize int
}

func (o Options) packetSize() int {
	if o.PacketSize <= 0 || o.PacketSize > maxPacketSize {
		return maxPacketSize
	}
	return o.PacketSize
}

// Active is the active mode of a socket, like the active option.
type Active = dgram.Active

var (
	// Passive leaves datagrams in the socket until Recv takes them, like
	// {active, false}. It is the default.
	Passive = dgram.Passive
	// Once sends the next datagram as a DataMsg, then turns passive, like
	// {active, once}.
	Once = dgram.Once
	// Always sends every datagram as a DataMsg, like {active, true}. A slow
	// owner sees its mailbox grow.
	Always = dgram.Always
)

// N sends the next n datagrams as DataMsg messages, then a PassiveMsg,
// turning passive, like {active, N}. Setting N on a socket already in this
// mode adds n to what is left; a total of zero or less turns it passive at
// once.
func N(n int) Active { return dgram.N(n) }
