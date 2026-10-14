package gentcp

import (
	"crypto/tls"
	"net"
	"testing"
)

// wrapped is a TCP connection wrapped, as one counting its reads is.
type wrapped struct{ *net.TCPConn }

// A TCP connection writes buffers at once, wrapped or not; a TLS one, or
// a pipe, joins them.
func TestVectored(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	for _, c := range []struct {
		name string
		conn net.Conn
		want bool
	}{
		{"tcp", conn, true},
		{"tcp wrapped", wrapped{conn.(*net.TCPConn)}, true},
		{"tls", tls.Client(conn, &tls.Config{}), false},
		{"pipe", a, false},
	} {
		if got := newWriter(c.conn, Options{}).vectored; got != c.want {
			t.Errorf("%s: vectored %v", c.name, got)
		}
	}
}
