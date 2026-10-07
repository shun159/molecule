package gentcp

import (
	"reflect"
	"testing"
)

func TestActive(t *testing.T) {
	for _, tt := range []struct {
		name    string
		from    Active
		to      Active
		want    Active
		passive bool
	}{
		{"once", Passive, Once, Once, false},
		{"n", Passive, N(3), N(3), false},
		{"n adds", N(2), N(3), N(5), false},
		{"n to zero", N(2), N(-2), Passive, true},
		{"n zero", Always, N(0), Passive, true},
		{"n after once replaces", Once, N(2), N(2), false},
		{"always", N(2), Always, Always, false},
		{"passive", Always, Passive, Passive, false},
	} {
		if got, p := tt.from.set(tt.to); got != tt.want || p != tt.passive {
			t.Errorf("%s: set = %+v, %v; want %+v, %v", tt.name, got, p, tt.want, tt.passive)
		}
	}

	a, p := Once.take()
	if a != Passive || p {
		t.Errorf("once.take = %+v, %v", a, p)
	}
	a, _ = N(2).take()
	if a, p = a.take(); a != Passive || !p {
		t.Errorf("N(2) taken twice = %+v, %v", a, p)
	}
	if a, p = Always.take(); a != Always || p {
		t.Errorf("always.take = %+v, %v", a, p)
	}
}

// cut feeds the pieces to a decoder and returns the packets it cuts.
func cut(t *testing.T, d decoder, length int, pieces ...string) ([]string, string, error) {
	t.Helper()
	var pkts []string
	for _, p := range pieces {
		d.feed([]byte(p))
		for {
			pkt, ok, err := d.next(length)
			if err != nil {
				return pkts, string(d.buf), err
			}
			if !ok {
				break
			}
			pkts = append(pkts, string(pkt))
		}
	}
	return pkts, string(d.buf), nil
}

func TestDecoder(t *testing.T) {
	for _, tt := range []struct {
		name   string
		d      decoder
		length int
		pieces []string
		pkts   []string
		left   string
		err    error
	}{
		{"raw takes all", decoder{}, 0, []string{"ab", "cde"}, []string{"ab", "cde"}, "", nil},
		{"raw exact", decoder{}, 3, []string{"ab", "cdefg"}, []string{"abc", "def"}, "g", nil},
		{"packet1", decoder{packet: Packet1}, 0, []string{"\x03ab", "c\x00\x01", "x"}, []string{"abc", "", "x"}, "", nil},
		{"packet2", decoder{packet: Packet2}, 0, []string{"\x00", "\x02hi\x00"}, []string{"hi"}, "\x00", nil},
		{"packet4", decoder{packet: Packet4}, 0, []string{"\x00\x00\x00\x05hel", "lo\x00\x00"}, []string{"hello"}, "\x00\x00", nil},
		{"packet4 too large", decoder{packet: Packet4, max: 4}, 0, []string{"\x00\x00\x00\x05"}, nil, "\x00\x00\x00\x05", ErrPacketTooLarge},
		{"line", decoder{packet: Line}, 0, []string{"ab\ncd", "\n\nx"}, []string{"ab\n", "cd\n", "\n"}, "x", nil},
		{"line too large", decoder{packet: Line, max: 3}, 0, []string{"abcd"}, nil, "abcd", ErrPacketTooLarge},
		{"line too large with newline", decoder{packet: Line, max: 3}, 0, []string{"abcd\n"}, nil, "abcd\n", ErrPacketTooLarge},
	} {
		pkts, left, err := cut(t, tt.d, tt.length, tt.pieces...)
		if !reflect.DeepEqual(pkts, tt.pkts) || left != tt.left || err != tt.err {
			t.Errorf("%s: %q left %q err %v; want %q left %q err %v", tt.name, pkts, left, err, tt.pkts, tt.left, tt.err)
		}
	}
}

// TestDecoderPeek checks that next on a copy leaves the original intact,
// and that a packet shares no memory with what follows it.
func TestDecoderPeek(t *testing.T) {
	d := decoder{packet: Line}
	d.feed([]byte("ab\ncd"))
	peek := d
	if _, ok, _ := peek.next(0); !ok || string(d.buf) != "ab\ncd" {
		t.Fatalf("peeking changed the decoder: %q", d.buf)
	}
	pkt, _, _ := d.next(0)
	d.feed([]byte("ef\n"))
	if string(pkt) != "ab\n" {
		t.Errorf("packet overwritten by later data: %q", pkt)
	}
}

func TestHeader(t *testing.T) {
	for _, tt := range []struct {
		p    Packet
		n    int
		want []byte
		err  error
	}{
		{Raw, 5, nil, nil},
		{Packet1, 255, []byte{255}, nil},
		{Packet1, 256, nil, ErrPacketTooLarge},
		{Packet2, 258, []byte{1, 2}, nil},
		{Packet4, 1 << 16, []byte{0, 1, 0, 0}, nil},
	} {
		if got, err := tt.p.header(tt.n); !reflect.DeepEqual(got, tt.want) || err != tt.err {
			t.Errorf("header(%v, %d) = %v, %v", tt.p, tt.n, got, err)
		}
	}
}
