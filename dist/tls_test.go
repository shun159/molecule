package dist_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/shun159/molecule/dist"
	"github.com/shun159/molecule/proc"
)

// ca is a certificate authority made for a test.
type ca struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newCA(t *testing.T, name string) *ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &ca{cert, key, pool}
}

// config returns the TLS configuration of a node with a certificate of
// c, trusting trust, the nodes proving themselves both ways.
func (c *ca) config(t *testing.T, node string, trust *ca) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: node},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		RootCAs:      trust.pool,
		ClientCAs:    trust.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
}

func TestTLS(t *testing.T) {
	c := newCA(t, "cluster")
	nodes, dists := clusterWith(t, map[string]dist.Config{
		"a@test": {Cookie: "secret", TLS: c.config(t, "a@test", c)},
		"b@test": {Cookie: "secret", TLS: c.config(t, "b@test", c)},
	})
	a, b := nodes[0], nodes[1]
	pa, ca := inbox(a)
	pb, cb := inbox(b)
	for i := range 10 {
		a.Send(pb, Ping{pa, i})
	}
	for i := range 10 {
		if m := next(t, cb); m != (Ping{pa, i}) {
			t.Fatalf("got %#v", m)
		}
	}
	b.Send(pa, "pong")
	if m := next(t, ca); m != "pong" {
		t.Errorf("got %#v", m)
	}
	if got := dists[1].Nodes(); len(got) != 1 {
		t.Errorf("b is connected to %v", got)
	}
}

// refused checks that a cannot connect to b, nor link to its processes.
func refused(t *testing.T, nodes []*proc.Node, dists []*dist.Dist) {
	t.Helper()
	if err := dists[0].Connect(context.Background(), "b@test"); !errors.Is(err, proc.NoConnection) {
		t.Errorf("Connect: %v", err)
	}
	pa, ca := inbox(nodes[0])
	pb, _ := inbox(nodes[1])
	in(nodes[0], pa, func(s *proc.Self) { s.Link(pb) })
	if m := next(t, ca); m != (proc.ExitMsg{From: pb, Reason: proc.NoConnection}) {
		t.Errorf("got %#v", m)
	}
}

func TestTLSOtherCA(t *testing.T) {
	ours, theirs := newCA(t, "ours"), newCA(t, "theirs")
	nodes, dists := clusterWith(t, map[string]dist.Config{
		"a@test": {Cookie: "secret", TLS: ours.config(t, "a@test", ours)},
		"b@test": {Cookie: "secret", TLS: theirs.config(t, "b@test", ours)},
	})
	refused(t, nodes, dists)
}

func TestTLSMixed(t *testing.T) {
	c := newCA(t, "cluster")
	nodes, dists := clusterWith(t, map[string]dist.Config{
		"a@test": {Cookie: "secret", TLS: c.config(t, "a@test", c)},
		"b@test": {Cookie: "secret"},
	})
	refused(t, nodes, dists)
	// Nor the other way.
	if err := dists[1].Connect(context.Background(), "a@test"); !errors.Is(err, proc.NoConnection) {
		t.Errorf("plain to TLS: %v", err)
	}
}

func TestTLSBadCookie(t *testing.T) {
	c := newCA(t, "cluster")
	nodes, dists := clusterWith(t, map[string]dist.Config{
		"a@test": {Cookie: "one", TLS: c.config(t, "a@test", c)},
		"b@test": {Cookie: "two", TLS: c.config(t, "b@test", c)},
	})
	refused(t, nodes, dists)
}
