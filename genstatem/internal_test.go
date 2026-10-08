package genstatem

import (
	"testing"
	"time"

	"github.com/shun159/molecule/gen"
	"github.com/shun159/molecule/proc"
)

// echoer returns the effects its casts carry, staying in its state.
type echoer struct{}

func (echoer) Init(proc.PID) (int, int, []gen.Effect, error) { return 0, 0, nil, nil }

func (echoer) HandleEvent(s, d int, ev Event) (int, int, []gen.Effect) {
	if c, ok := ev.(Cast); ok {
		effs, _ := c.Msg.([]gen.Effect)
		return s, d, effs
	}
	return s, d, nil
}

func hasCancel(effs []gen.Effect, key any) bool {
	for _, e := range effs {
		if c, ok := e.(gen.CancelTimer); ok && c.Key == key {
			return true
		}
	}
	return false
}

// TestEventTimeoutFiredIsOver checks, on the pure adapter, that an event
// timeout that fired is not cancelled again by the next event, while a
// running one is.
func TestEventTimeoutFiredIsOver(t *testing.T) {
	a := newAdapter[int, int](echoer{})
	m, _, _ := a.Init(proc.PID{}, nil)
	arm := gen.CastMsg{Req: gen.Do(StartEventTimeout{After: time.Second, Msg: "t"})}

	m, _ = a.Handle(m, arm)
	if m, effs := a.Handle(m, gen.CastMsg{}); !hasCancel(effs, eventTimerKey{}) || m.eventTimer {
		t.Errorf("a running event timeout not cancelled by an event: %#v", effs)
	}

	m, _ = a.Handle(m, arm)
	m, effs := a.Handle(m, gen.InfoMsg{Msg: fired{eventTimerKey{}}})
	if hasCancel(effs, eventTimerKey{}) {
		t.Errorf("the event timeout that fired cancelled: %#v", effs)
	}
	if _, effs := a.Handle(m, gen.CastMsg{}); hasCancel(effs, eventTimerKey{}) {
		t.Errorf("a fired event timeout cancelled by the next event: %#v", effs)
	}
}
