package gentcpacceptor

import (
	"errors"
	"reflect"
	"testing"

	"github.com/shun159/molecule"
	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/gentcp"
	"github.com/shun159/molecule/proc"
)

// recorder is a pure Behaviour recording what it is given.
type recorder struct{}

type rstate struct {
	data   []string
	closed []error
	infos  []any
}

func (recorder) Init(proc.PID, gentcp.Socket) (rstate, []molecule.Effect, error) {
	return rstate{}, nil, nil
}

func (recorder) HandleData(s rstate, sock gentcp.Socket, b []byte) (rstate, []molecule.Effect) {
	s.data = append(s.data[:len(s.data):len(s.data)], string(b))
	return s, molecule.Do(sock.SendEffect(b))
}

func (recorder) HandleInfo(s rstate, _ gentcp.Socket, msg any) (rstate, []molecule.Effect) {
	s.infos = append(s.infos[:len(s.infos):len(s.infos)], msg)
	return s, nil
}

// closer also records the closing of the connection.
type closer struct{ recorder }

func (closer) HandleClosed(s rstate, _ gentcp.Socket, err error) (rstate, []molecule.Effect) {
	s.closed = append(s.closed[:len(s.closed):len(s.closed)], err)
	return s, nil
}

// minimal implements only the required callbacks.
type minimal struct{}

func (minimal) Init(proc.PID, gentcp.Socket) (int, []molecule.Effect, error) { return 0, nil, nil }
func (minimal) HandleData(n int, _ gentcp.Socket, _ []byte) (int, []molecule.Effect) {
	return n + 1, nil
}

func info(m any) gen.Msg { return gen.InfoMsg{Msg: m} }

func TestAdapter(t *testing.T) {
	n := proc.NewNode("")
	sock := gentcp.Socket{PID: n.NewPID()}
	other := gentcp.Socket{PID: n.NewPID()}
	a := adapter[rstate]{b: recorder{}, activeN: 3}

	c, effs := a.Handle(initial(a), info("before attach"))
	if c.ready || effs != nil {
		t.Errorf("before attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(gentcp.DataMsg{Sock: sock, Bytes: []byte("early")}))
	if c.ready || effs != nil {
		t.Errorf("data before attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(attached{sock}))
	if !c.ready || !reflect.DeepEqual(effs, molecule.Do(sock.SetActiveEffect(gentcp.N(3)))) {
		t.Errorf("on attach: %+v, %#v", c, effs)
	}
	c, effs = a.Handle(c, info(gentcp.DataMsg{Sock: sock, Bytes: []byte("hi")}))
	if want := molecule.Do(sock.SendEffect([]byte("hi")), sock.SetActiveEffect(gentcp.N(1))); !reflect.DeepEqual(effs, want) {
		t.Errorf("on data: %#v, want the write then one more packet", effs)
	}
	c, effs = a.Handle(c, info(gentcp.DataMsg{Sock: other, Bytes: []byte("stray")}))
	if effs != nil || len(c.state.data) != 1 {
		t.Errorf("data from another socket handled: %+v", c.state)
	}
	c, effs = a.Handle(c, info(gentcp.PassiveMsg{Sock: sock}))
	if effs != nil || len(c.state.infos) != 0 {
		t.Errorf("own Passive handled: %+v, %#v", c.state, effs)
	}
	cast := molecule.CastMsg{Req: "hello"}
	c, _ = a.Handle(c, cast)
	c, _ = a.Handle(c, info("plain"))
	c, _ = a.Handle(c, info(gentcp.PassiveMsg{Sock: other}))
	if want := []any{cast, "plain", gentcp.PassiveMsg{Sock: other}}; !reflect.DeepEqual(c.state.infos, want) {
		t.Errorf("infos = %#v", c.state.infos)
	}

	boom := errors.New("boom")
	if _, effs = a.Handle(c, info(gentcp.ClosedMsg{Sock: other})); effs != nil {
		t.Errorf("closing of another socket handled: %#v", effs)
	}
	stopped, effs := a.Handle(c, info(gentcp.ErrorMsg{Sock: sock, Err: boom}))
	if stop, ok := effs[0].(molecule.Stop); len(effs) != 1 || !ok ||
		!errors.Is(stop.Reason, proc.Shutdown) || !errors.Is(stop.Reason, boom) {
		t.Errorf("on an error: %#v, want a stop with a shutdown wrapping it", effs)
	}
	if _, effs = a.Handle(stopped, info(gentcp.ClosedMsg{Sock: sock})); effs != nil {
		t.Errorf("Closed after Error: %#v, want nothing more", effs)
	}
	if _, effs = a.Handle(c, info(gentcp.ClosedMsg{Sock: sock})); !reflect.DeepEqual(effs, molecule.Do(molecule.Stop{})) {
		t.Errorf("on closed by the peer: %#v", effs)
	}

	m := adapter[int]{b: minimal{}, activeN: 1}
	mc, _ := m.Handle(initial(m), info(attached{sock}))
	if mc, effs = m.Handle(mc, info(gentcp.DataMsg{Sock: sock})); !reflect.DeepEqual(effs, molecule.Do(sock.SetActiveEffect(gentcp.N(1)))) {
		t.Errorf("on data without effects: %#v, want one more packet", effs)
	}
	if mc, effs = m.Handle(mc, info("ignored")); effs != nil || mc.state != 1 {
		t.Errorf("info without InfoHandler: %+v, %#v", mc, effs)
	}
}

func TestAdapterClosed(t *testing.T) {
	sock := gentcp.Socket{PID: proc.NewNode("").NewPID()}
	a := adapter[rstate]{b: closer{}, activeN: 1}
	c, _ := a.Handle(initial(a), info(attached{sock}))
	boom := errors.New("boom")
	c, effs := a.Handle(c, info(gentcp.ErrorMsg{Sock: sock, Err: boom}))
	c, _ = a.Handle(c, info(gentcp.ClosedMsg{Sock: sock}))
	if effs != nil || !reflect.DeepEqual(c.state.closed, []error{boom}) {
		t.Errorf("Error then Closed: %#v, %v; want one HandleClosed", effs, c.state.closed)
	}
	c, _ = a.Handle(initial(a), info(attached{sock}))
	c, _ = a.Handle(c, info(gentcp.ClosedMsg{Sock: sock}))
	if !reflect.DeepEqual(c.state.closed, []error{nil}) {
		t.Errorf("Closed: %v", c.state.closed)
	}
}

// terminator reports Terminate through its effects.
type terminator struct{ minimal }

func (terminator) Terminate(n int, reason error) []molecule.Effect {
	return molecule.Do(molecule.Stop{Reason: reason})
}

func TestAdapterTerminate(t *testing.T) {
	a := adapter[int]{b: terminator{}, activeN: 1}
	boom := errors.New("boom")
	if effs := a.Terminate(initial(a), boom); effs != nil {
		t.Errorf("Terminate before Init: %#v", effs)
	}
	c, _ := a.Handle(initial(a), info(attached{gentcp.Socket{}}))
	if effs := a.Terminate(c, boom); !reflect.DeepEqual(effs, molecule.Do(molecule.Stop{Reason: boom})) {
		t.Errorf("Terminate after Init: %#v", effs)
	}
	if effs := (adapter[int]{b: minimal{}, activeN: 1}).Terminate(c, boom); effs != nil {
		t.Errorf("Terminate without Terminator: %#v", effs)
	}
}

// initial is the state of a connection process once started, as the
// runtime makes it with Init.
func initial[S any](a adapter[S]) conn[S] {
	c, _, _ := a.Init(proc.PID{}, nil)
	return c
}
