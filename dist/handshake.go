package dist

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// The handshake, on the bare connection, in packets of a 4-byte length:
//
//	dialer   → acceptor  hello:   version, name, creation, challenge
//	acceptor → dialer    welcome: name, creation, challenge, proof
//	                     or refuse: why
//	dialer   → acceptor  proof
//
// A proof is the HMAC-SHA256 of the challenge of the other side, keyed by
// the cookie, and of which side proves: each side shows it has the cookie
// without sending it.

const (
	version       = 1
	challengeSize = 16
	maxHandshake  = 1 << 12

	tagHello   = 'H'
	tagWelcome = 'W'
	tagRefuse  = 'R'
	tagProof   = 'P'
)

// handshakeTimeout bounds a handshake.
var handshakeTimeout = 10 * time.Second

// ErrRefused is the error of a handshake the other node refused.
var ErrRefused = errors.New("dist: connection refused")

var errBadCookie = errors.New("dist: wrong cookie")

// refuseSimultaneous is why a node refuses a dial while its own dial to
// the dialer, which wins, is in progress: the dialer is to wait for it.
const refuseSimultaneous = "simultaneous connection"

var errSimultaneous = fmt.Errorf("%w: %s", ErrRefused, refuseSimultaneous)

// peerInfo is what a handshake tells of the other node.
type peerInfo struct {
	name     string
	creation uint32
}

type hello struct {
	name      string
	creation  uint32
	challenge []byte
}

func appendHello(b []byte, tag byte, h hello) []byte {
	b = append(b, tag)
	if tag == tagHello {
		b = append(b, version)
	}
	b = appendString(b, h.name)
	b = binary.BigEndian.AppendUint32(b, h.creation)
	return append(b, h.challenge...)
}

func parseHello(b []byte, tag byte) (h hello, rest []byte, err error) {
	if len(b) == 0 || b[0] != tag {
		return h, nil, errFrame
	}
	b = b[1:]
	if tag == tagHello {
		if len(b) == 0 || b[0] != version {
			return h, nil, fmt.Errorf("dist: unsupported version")
		}
		b = b[1:]
	}
	r := reader{b: b}
	h.name = r.string()
	if r.err != nil || len(r.b) < 4+challengeSize {
		return h, nil, errFrame
	}
	h.creation = binary.BigEndian.Uint32(r.b)
	h.challenge = r.b[4 : 4+challengeSize]
	return h, r.b[4+challengeSize:], nil
}

func newChallenge() []byte {
	c := make([]byte, challengeSize)
	rand.Read(c)
	return c
}

func proof(cookie string, side string, challenge []byte) []byte {
	m := hmac.New(sha256.New, []byte(cookie))
	m.Write([]byte(side))
	m.Write(challenge)
	return m.Sum(nil)
}

// dialHandshake runs the handshake of the dialer on conn.
func dialHandshake(conn net.Conn, self hello, cookie string) (peerInfo, error) {
	conn.SetDeadline(time.Now().Add(handshakeTimeout))
	defer conn.SetDeadline(time.Time{})
	if err := writePacket(conn, appendHello(nil, tagHello, self)); err != nil {
		return peerInfo{}, err
	}
	b, err := readPacket(conn)
	if err != nil {
		return peerInfo{}, err
	}
	if len(b) > 0 && b[0] == tagRefuse {
		if string(b[1:]) == refuseSimultaneous {
			return peerInfo{}, errSimultaneous
		}
		return peerInfo{}, fmt.Errorf("%w: %s", ErrRefused, b[1:])
	}
	w, rest, err := parseHello(b, tagWelcome)
	if err != nil {
		return peerInfo{}, err
	}
	if !hmac.Equal(rest, proof(cookie, "accept", self.challenge)) {
		return peerInfo{}, errBadCookie
	}
	p := proof(cookie, "dial", w.challenge)
	if err := writePacket(conn, append([]byte{tagProof}, p...)); err != nil {
		return peerInfo{}, err
	}
	return peerInfo{w.name, w.creation}, nil
}

// acceptHandshake runs the handshake of the acceptor on conn. decide is
// told who dials, and returns why to refuse, if it does.
func acceptHandshake(conn net.Conn, self hello, cookie string, decide func(peerInfo) (refuse string)) (peerInfo, error) {
	conn.SetDeadline(time.Now().Add(handshakeTimeout))
	defer conn.SetDeadline(time.Time{})
	b, err := readPacket(conn)
	if err != nil {
		return peerInfo{}, err
	}
	h, _, err := parseHello(b, tagHello)
	if err != nil {
		return peerInfo{}, err
	}
	info := peerInfo{h.name, h.creation}
	if why := decide(info); why != "" {
		writePacket(conn, append([]byte{tagRefuse}, why...))
		return info, fmt.Errorf("%w: %s", ErrRefused, why)
	}
	welcome := appendHello(nil, tagWelcome, self)
	welcome = append(welcome, proof(cookie, "accept", h.challenge)...)
	if err := writePacket(conn, welcome); err != nil {
		return info, err
	}
	b, err = readPacket(conn)
	if err != nil {
		return info, err
	}
	if len(b) == 0 || b[0] != tagProof || !hmac.Equal(b[1:], proof(cookie, "dial", self.challenge)) {
		return info, errBadCookie
	}
	return info, nil
}

func writePacket(w io.Writer, b []byte) error {
	_, err := w.Write(binary.BigEndian.AppendUint32(nil, uint32(len(b))))
	if err == nil {
		_, err = w.Write(b)
	}
	return err
}

func readPacket(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	l := binary.BigEndian.Uint32(hdr[:])
	if l > maxHandshake {
		return nil, errFrame
	}
	b := make([]byte, l)
	_, err := io.ReadFull(r, b)
	return b, err
}
