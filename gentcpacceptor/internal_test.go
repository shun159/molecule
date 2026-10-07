package gentcpacceptor

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// recorder is a pure Behaviour recording what it is given.
type recorder struct{}

type rstate struct {
	data   []string
	closed []error
	infos  []any
}

func (recorder) Init(Socket) (rstate, []gen.Effect, error) { return rstate{}, nil, nil }

func (recorder) HandleData(s rstate, sock Socket, b []byte) (rstate, []gen.Effect) {
	s.data = append(s.data[:len(s.data):len(s.data)], string(b))
	return s, gen.Do(sock.Write(b))
}

func (recorder) HandleInfo(s rstate, _ Socket, msg any) (rstate, []gen.Effect) {
	s.infos = append(s.infos[:len(s.infos):len(s.infos)], msg)
	return s, nil
}

// minimal implements only the required callbacks.
type minimal struct{}

func (minimal) Init(Socket) (int, []gen.Effect, error)                   { return 0, nil, nil }
func (minimal) HandleData(n int, _ Socket, _ []byte) (int, []gen.Effect) { return n + 1, nil }

func info(m any) gen.Msg { return gen.InfoMsg{Msg: m} }

func TestAdapter(t *testing.T) {
	pid := proc.NewNode("").Spawn(func(*proc.Self) error { return nil }) // any PID will do
	sock := Socket{PID: pid}
	a := adapter[rstate]{recorder{}}

	c, effs := a.Handle(conn[rstate]{}, info("before attach"))
	if c.ready || effs != nil {
		t.Errorf("before attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(attached{sock}))
	if !c.ready || !reflect.DeepEqual(effs, gen.Do(sock.activeOnce())) {
		t.Errorf("on attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(data{sock: pid, b: []byte("hi")}))
	if want := gen.Do(sock.Write([]byte("hi")), sock.activeOnce()); !reflect.DeepEqual(effs, want) {
		t.Errorf("on data: %#v, want the write then a new read", effs)
	}
	c, effs = a.Handle(c, info(data{sock: proc.PID{}, b: []byte("stray")}))
	if effs != nil || len(c.state.data) != 1 {
		t.Errorf("data from another socket handled: %+v", c.state)
	}
	cast := gen.CastMsg{Req: "hello"}
	c, _ = a.Handle(c, cast)
	c, _ = a.Handle(c, info("plain"))
	if !reflect.DeepEqual(c.state.infos, []any{cast, "plain"}) {
		t.Errorf("infos = %#v", c.state.infos)
	}
	boom := errors.New("boom")
	if _, effs = a.Handle(c, info(closed{sock: pid, err: boom})); !reflect.DeepEqual(effs, gen.Do(gen.Stop{Reason: boom})) {
		t.Errorf("on closed with an error: %#v", effs)
	}
	if _, effs = a.Handle(c, info(closed{sock: pid})); !reflect.DeepEqual(effs, gen.Do(gen.Stop{})) {
		t.Errorf("on closed by the peer: %#v", effs)
	}

	m := adapter[int]{minimal{}}
	mc, _ := m.Handle(conn[int]{}, info(attached{sock}))
	if mc, effs = m.Handle(mc, info("ignored")); effs != nil || mc.state != 0 {
		t.Errorf("info without InfoHandler: %+v, %#v", mc, effs)
	}
}

// TestSocketReadsOnDemand checks that the socket reads only when asked,
// so a peer cannot flood its owner.
func TestSocketReadsOnDemand(t *testing.T) {
	n := proc.NewNode("")
	got := make(chan any, 8)
	owner := n.Spawn(func(s *proc.Self) error {
		for {
			msg, err := s.Receive(context.Background())
			if err != nil {
				return err
			}
			got <- msg
		}
	})
	server, peer := net.Pipe()
	defer peer.Close()
	sock := n.Spawn(func(s *proc.Self) error { return runSocket(s, server, owner) })

	written := make(chan struct{})
	go func() {
		peer.Write([]byte("one"))
		peer.Write([]byte("two")) // blocks until the socket reads again
		close(written)
	}()
	expectNone := func() {
		t.Helper()
		select {
		case m := <-got:
			t.Fatalf("read without being asked: %#v", m)
		case <-time.After(50 * time.Millisecond):
		}
	}

	expectNone()
	n.Send(sock, activeOnce{})
	if m := (<-got).(data); string(m.b) != "one" {
		t.Fatalf("got %q", m.b)
	}
	expectNone()
	select {
	case <-written:
		t.Fatal("second write went through without a read")
	default:
	}
	n.Send(sock, activeOnce{})
	if m := (<-got).(data); string(m.b) != "two" {
		t.Fatalf("got %q", m.b)
	}

	peer.Close()
	n.Send(sock, activeOnce{})
	if m := (<-got).(closed); m.err != nil || m.sock != sock {
		t.Errorf("on peer close: %#v", m)
	}
}

// terminator reports Terminate through its effects.
type terminator struct{ minimal }

func (terminator) Terminate(n int, reason error) []gen.Effect {
	return gen.Do(gen.Stop{Reason: reason})
}

func TestAdapterTerminate(t *testing.T) {
	a := adapter[int]{terminator{}}
	boom := errors.New("boom")
	if effs := a.Terminate(conn[int]{}, boom); effs != nil {
		t.Errorf("Terminate before Init: %#v", effs)
	}
	c, _ := a.Handle(conn[int]{}, info(attached{Socket{}}))
	if effs := a.Terminate(c, boom); !reflect.DeepEqual(effs, gen.Do(gen.Stop{Reason: boom})) {
		t.Errorf("Terminate after Init: %#v", effs)
	}
	if effs := (adapter[int]{minimal{}}).Terminate(c, boom); effs != nil {
		t.Errorf("Terminate without Terminator: %#v", effs)
	}
}
