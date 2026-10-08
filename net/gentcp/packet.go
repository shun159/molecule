package gentcp

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Packet is how the bytes of a connection are cut into packets, like the
// packet option of gen_tcp.
type Packet int

const (
	// Raw does not cut: a packet is whatever was read.
	Raw Packet = iota
	// Packet1, Packet2 and Packet4 prefix each packet with its length, on
	// 1, 2 or 4 bytes, big-endian. Send adds the prefix, and a received
	// packet comes without it.
	Packet1
	Packet2
	Packet4
	// Line cuts after each newline, which the packet keeps.
	Line
)

// ErrPacketTooLarge is the error of a packet longer than PacketSize, or
// than its length prefix can tell.
var ErrPacketTooLarge = errors.New("gentcp: packet too large")

func (p Packet) prefix() int {
	switch p {
	case Packet1:
		return 1
	case Packet2:
		return 2
	case Packet4:
		return 4
	}
	return 0
}

// header returns the prefix of a packet of n bytes, empty without one.
func (p Packet) header(n int) ([]byte, error) {
	switch p {
	case Packet1:
		if n > 0xff {
			return nil, ErrPacketTooLarge
		}
		return []byte{byte(n)}, nil
	case Packet2:
		if n > 0xffff {
			return nil, ErrPacketTooLarge
		}
		return binary.BigEndian.AppendUint16(nil, uint16(n)), nil
	case Packet4:
		if uint64(n) > 0xffffffff {
			return nil, ErrPacketTooLarge
		}
		return binary.BigEndian.AppendUint32(nil, uint32(n)), nil
	}
	return nil, nil
}

// decoder keeps the bytes read and cuts packets out of them. It is a
// value: next on a copy leaves the original as it was, which peeks.
type decoder struct {
	packet Packet
	max    int // 0: no limit
	buf    []byte
}

// feed adds bytes read, which the decoder then owns.
func (d *decoder) feed(b []byte) {
	if len(d.buf) == 0 {
		d.buf = b
		return
	}
	d.buf = append(d.buf, b...)
}

// next cuts the next packet, if a whole one is there. For Raw, length 0
// takes all there is, and a positive length exactly that many bytes; the
// other packet types ignore length. A packet shares no memory with what
// is left.
func (d *decoder) next(length int) (pkt []byte, ok bool, err error) {
	var end, from, to int // the packet is buf[from:to], what is left buf[end:]
	switch d.packet {
	case Raw:
		switch {
		case len(d.buf) == 0 || len(d.buf) < length:
			return nil, false, nil
		case length == 0:
			end = len(d.buf)
		default:
			end = length
		}
		from, to = 0, end
	case Line:
		i := bytes.IndexByte(d.buf, '\n')
		if i < 0 {
			if d.max > 0 && len(d.buf) > d.max {
				return nil, false, ErrPacketTooLarge
			}
			return nil, false, nil
		}
		end = i + 1
		from, to = 0, end
	default:
		h := d.packet.prefix()
		if len(d.buf) < h {
			return nil, false, nil
		}
		var size uint64
		for _, b := range d.buf[:h] {
			size = size<<8 | uint64(b)
		}
		if d.max > 0 && size > uint64(d.max) {
			return nil, false, ErrPacketTooLarge
		}
		if uint64(len(d.buf)-h) < size {
			return nil, false, nil
		}
		from, to = h, h+int(size)
		end = to
	}
	if d.packet == Line && d.max > 0 && to-from > d.max {
		return nil, false, ErrPacketTooLarge
	}
	pkt = d.buf[from:to:to]
	if end == len(d.buf) {
		d.buf = nil
	} else {
		d.buf = d.buf[end:]
	}
	return pkt, true, nil
}
