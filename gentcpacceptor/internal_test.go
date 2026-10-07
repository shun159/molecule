package gentcpacceptor

import (
	"bytes"
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

func (recorder) Init(proc.PID, Socket) (rstate, []gen.Effect, error) { return rstate{}, nil, nil }

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

func (minimal) Init(proc.PID, Socket) (int, []gen.Effect, error)         { return 0, nil, nil }
func (minimal) HandleData(n int, _ Socket, _ []byte) (int, []gen.Effect) { return n + 1, nil }

func info(m any) gen.Msg { return gen.InfoMsg{Msg: m} }

func TestAdapter(t *testing.T) {
	pid := proc.NewNode("").Spawn(func(*proc.Self) error { return nil }) // any PID will do
	sock := Socket{PID: pid}
	a := adapter[rstate]{b: recorder{}, activeN: 3}

	c, effs := a.Handle(initial(a), info("before attach"))
	if c.ready || effs != nil {
		t.Errorf("before attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(attached{sock}))
	if !c.ready || !reflect.DeepEqual(effs, gen.Do(sock.active(3))) {
		t.Errorf("on attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(data{sock: pid, b: []byte("hi")}))
	if want := gen.Do(sock.Write([]byte("hi")), sock.active(1)); !reflect.DeepEqual(effs, want) {
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
	_, effs = a.Handle(c, info(closed{sock: pid, err: boom}))
	if stop, ok := effs[0].(gen.Stop); len(effs) != 1 || !ok ||
		!errors.Is(stop.Reason, proc.Shutdown) || !errors.Is(stop.Reason, boom) {
		t.Errorf("on closed with an error: %#v, want a stop with a shutdown wrapping it", effs)
	}
	if _, effs = a.Handle(c, info(closed{sock: pid})); !reflect.DeepEqual(effs, gen.Do(gen.Stop{})) {
		t.Errorf("on closed by the peer: %#v", effs)
	}

	m := adapter[int]{b: minimal{}, activeN: 1}
	mc, _ := m.Handle(initial(m), info(attached{sock}))
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
	n.Send(sock, active{1})
	if m := (<-got).(data); string(m.b) != "one" {
		t.Fatalf("got %q", m.b)
	}
	expectNone()
	select {
	case <-written:
		t.Fatal("second write went through without a read")
	default:
	}
	n.Send(sock, active{1})
	if m := (<-got).(data); string(m.b) != "two" {
		t.Fatalf("got %q", m.b)
	}

	peer.Close()
	n.Send(sock, active{1})
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
	a := adapter[int]{b: terminator{}, activeN: 1}
	boom := errors.New("boom")
	if effs := a.Terminate(initial(a), boom); effs != nil {
		t.Errorf("Terminate before Init: %#v", effs)
	}
	c, _ := a.Handle(initial(a), info(attached{Socket{}}))
	if effs := a.Terminate(c, boom); !reflect.DeepEqual(effs, gen.Do(gen.Stop{Reason: boom})) {
		t.Errorf("Terminate after Init: %#v", effs)
	}
	if effs := (adapter[int]{b: minimal{}, activeN: 1}).Terminate(c, boom); effs != nil {
		t.Errorf("Terminate without Terminator: %#v", effs)
	}
}

// TestSocketActiveN allows two reads ahead: both happen without waiting
// for the owner, the third does not.
func TestSocketActiveN(t *testing.T) {
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
	go func() {
		for _, w := range []string{"one", "two", "three"} {
			peer.Write([]byte(w))
		}
	}()

	n.Send(sock, active{2})
	first := receive[data](t, got)
	second := receive[data](t, got)
	if string(first.b) != "one" || string(second.b) != "two" {
		t.Fatalf("got %q, %q", first.b, second.b)
	}
	select {
	case m := <-got:
		t.Fatalf("read beyond the allowance: %#v", m)
	case <-time.After(50 * time.Millisecond):
	}
	n.Send(sock, active{1})
	if third := (<-got).(data); string(third.b) != "three" {
		t.Fatalf("got %q", third.b)
	}
	// What was delivered is the owner's: later reads do not overwrite it.
	if string(first.b) != "one" || string(second.b) != "two" {
		t.Errorf("delivered data overwritten: %q, %q", first.b, second.b)
	}
}

func TestNextReadSize(t *testing.T) {
	for _, tt := range []struct{ size, n, want int }{
		{minReadBuffer, minReadBuffer, 2 * minReadBuffer}, // full: grow
		{maxReadBuffer, maxReadBuffer, maxReadBuffer},     // full at the top: stay
		{8 << 10, 4 << 10, 8 << 10},                       // half: stay
		{8 << 10, 1 << 10, 4 << 10},                       // under a quarter: shrink
		{minReadBuffer, 0, minReadBuffer},                 // at the bottom: stay
		{16 << 10, 16<<10 - 1, 16 << 10},                  // nearly full: stay
	} {
		if got := nextReadSize(tt.size, tt.n); got != tt.want {
			t.Errorf("nextReadSize(%d, %d) = %d, want %d", tt.size, tt.n, got, tt.want)
		}
	}
}

// receive returns the next message from got, failing after a while.
func receive[T any](t *testing.T, got <-chan any) T {
	t.Helper()
	select {
	case m := <-got:
		return m.(T)
	case <-time.After(5 * time.Second):
		t.Fatal("nothing delivered")
		panic("unreachable")
	}
}

// TestSocketHandsOverFullReads reads chunks filling the buffer, which are
// handed over without a copy: the next read must go to a new buffer.
func TestSocketHandsOverFullReads(t *testing.T) {
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

	chunks := [][]byte{bytes.Repeat([]byte("a"), minReadBuffer), bytes.Repeat([]byte("b"), minReadBuffer)}
	go func() {
		for _, c := range chunks {
			peer.Write(c)
		}
	}()
	n.Send(sock, active{2})
	first := receive[data](t, got)
	second := receive[data](t, got)
	if !bytes.Equal(first.b, chunks[0]) || !bytes.Equal(second.b, chunks[1]) {
		t.Errorf("first read now %q..., second %q...", first.b[:4], second.b[:4])
	}
}

// initial is the state of a connection process once started, as the
// runtime makes it with Init.
func initial[S any](a adapter[S]) conn[S] {
	c, _, _ := a.Init(proc.PID{}, nil)
	return c
}
